# Deploying par-rt-db to a Docker host

Runbook for operating par-rt-db in production: deploying and updating the
server, configuring the Cloudflare tunnel and Postgres image, monitoring and
tracing, backups and restore, and rolling back a bad deploy.

The target is a standalone Docker host (plain `docker compose`, not Swarm). Public
traffic reaches it through the host `cloudflared` tunnel, which routes
`rtdb.example.com` -> `http://localhost:8300`. No ports 80/443 are opened on the
VPS and no reverse proxy is needed — TLS is terminated at Cloudflare's edge.

## Table of contents

- [One-time DNS/tunnel wiring (already done 2026-07-21)](#one-time-dnstunnel-wiring)
- [Deploy / update](#deploy--update)
- [Host reservation (quiet window)](#host-reservation-quiet-window)
- [Topology: single instance by default; multi-instance is opt-in](#topology-single-instance-by-default-multi-instance-is-opt-in)
- [Host machine authorization rollout](#host-machine-authorization-rollout)
- [Postgres image](#postgres-image)
- [Collation](#collation)
- [Subscription-invalidation observability](#subscription-invalidation-observability)
- [Monitoring](#monitoring)
- [Tracing (OpenTelemetry / OTLP, ENH-018)](#tracing-opentelemetry--otlp-enh-018)
- [Secrets (`/docker/par-rt-db/.env`, not committed)](#secrets-dockerpar-rt-dbenv-not-committed)
- [Dashboard / SPA](#dashboard--spa)
- [Admin bootstrap (after first deploy)](#admin-bootstrap-after-first-deploy)
- [Hot config beats env](#hot-config-beats-env)
- [Backups & restore](#backups--restore)
- [Troubleshooting](#troubleshooting)
- [Rollback](#rollback)

## One-time DNS/tunnel wiring

- Proxied CNAME `rtdb.example.com` -> `<tunnel-id>.cfargotunnel.com`
  (overrides the `*.example.com` wildcard that points at another host).
- Tunnel ingress rule `rtdb.example.com` -> `http://localhost:8300` appended to
  the docker-host tunnel.
- No Cloudflare Access app — par-rt-db carries its own auth.

## Deploy / update

The default path is continuous deployment (adopted 2026-10-25, the "Lenny2 CD
recipe" established across par-hack, read-it-to-me, and par-artifacts): every
green push to `main` runs the CI `deploy` job, which SSHes to the deploy host
with a forced-command key and rolls that exact commit. The private key lives
only in the repo's `LENNY2_SSH_KEY` GitHub secret; on the host it is
half of a keypair whose `authorized_keys` line restricts it to
`command="/usr/local/bin/parrtdb-deploy $SSH_ORIGINAL_COMMAND",restrict` — the
key can do nothing but run that script, and the script accepts ONLY a bare
40-hex SHA as its argument (anything else exits 2). Exactly one SSH invocation
happens per deploy. The script re-runs the deploy-path invariants: the
host-reservation lease check (an active quiet-window lease BLOCKS the deploy),
git fetch + `checkout -f <sha>` in `/docker/par-rt-db`, the constrained
`par-rt-db-builder` build (`RTDB_BUILD_COMMIT` baked in, builder clamped to
two CPUs during the build and restored after, including on failure), and a
bounded `/healthz` poll (5-minute ceiling) that fails the job if the rollout
never serves.

The manual path remains `make deploy` from the repo root: it gates via
`make checkall-cached` (reusing the green verdict stamped by a previous
`make checkall` for the identical tree content — any edit to a tracked file
changes the git tree hash and forces a full re-gate), then rsyncs to the
deploy host and runs
`BUILDX_BUILDER=par-rt-db-builder docker compose up -d --build` with
`RTDB_BUILD_COMMIT` baked in. See
the [`Makefile`](../Makefile) `deploy` target for the canonical commands.
Use it for out-of-band deploys (e.g. with an emergency
`RESERVATION_OVERRIDE=1`) or when the CD path is unavailable; the CD path is
authoritative for ordinary main pushes.

The deploy target bootstraps the named `par-rt-db-builder` BuildKit
`docker-container` builder if it does not already exist. The builder reads
[`buildkitd.toml`](./buildkitd.toml), which limits solver parallelism to two
steps, and is constrained to four CPUs with `cpu-quota=400000` and
`cpu-period=100000` in its steady state. `default-load=true` makes the
resulting image available to the Docker Engine used by Compose. The existing
builder is reused so deploys retain its cache and do not recreate a running
builder.

Before the image build starts, the deploy target constrains the builder
container to two CPUs (`docker update --cpu-quota 200000`) and restores the
four-CPU steady state as soon as the build exits, including on failure. A cold
Rust build at the full four-CPU quota coincided with host load 59.6 and
shared-endpoint 502s (2026-10-16); the identical cold build under the two-CPU
build window held host load at or below 11.4 with every sampled endpoint
serving. The deploy aborts before building if the builder container cannot be
resolved or constrained — it never builds unconstrained.

These limits reserve CPU capacity for the host-local `cloudflared` connector
while Rust and dashboard build steps run. Verify availability under the current
build and host workload with the monitoring procedure below. The limits bound
both BuildKit solver concurrency and the builder container's CPU share. A Docker Compose service `deploy.resources`
limit would not provide this protection because it applies only after an image
has been built.

For a live verification, run the deploy in the background and sample the host
load plus every tunnel hostname until that deploy exits. The monitor is
bounded: 540 samples at 5s covers 45 minutes, a trap tears the background
deploy down on interrupt, and the closing wait only runs once the deploy has
exited, so a hung deploy cannot block it forever:

```sh
hosts="app-1.example.com app-2.example.com app-3.example.com \
rtdb.example.com app-4.example.com app-5.example.com"
make deploy DEPLOY_HOST=root@docker-host.example.com \
  >/tmp/par-rt-db-deploy.log 2>&1 &
deploy_pid=$!
cleanup() {
  kill "$deploy_pid" 2>/dev/null || true
  wait "$deploy_pid" 2>/dev/null || true
}
trap cleanup INT TERM
# 540 samples x 5s = 45 minutes of coverage; break early once the deploy exits.
for sample in $(seq 1 540); do
  kill -0 "$deploy_pid" 2>/dev/null || break
  date -Is
  ssh root@docker-host.example.com 'uptime'
  for host in $hosts; do
    curl --fail --silent --show-error --output /dev/null \
      --write-out "$host %{http_code}\n" "https://$host/"
  done
  sleep 5
done
# Bounded finish: terminate a deploy that exceeds the 45-minute window, then
# reap it so this monitor cannot block forever.
if kill -0 "$deploy_pid" 2>/dev/null; then
  echo "monitor window elapsed; terminating deploy $deploy_pid"
  kill "$deploy_pid" 2>/dev/null || true
  wait "$deploy_pid" 2>/dev/null || true
  exit 124
fi
wait "$deploy_pid"
deploy_rc=$?
echo "deploy exited with status $deploy_rc"
exit "$deploy_rc"
```

Inspect `/tmp/par-rt-db-deploy.log` and the terminal output. The acceptance
threshold is load below 16 on every sample and HTTP 200 from
`app-2.example.com`, `app-3.example.com`, `rtdb.example.com`, and
`app-5.example.com` throughout the build window. The additional tunnel
hostnames must remain healthy too. Do not mark the mitigation verified from a
local `docker compose config` check alone.

The manual path below must use the same builder bootstrap and two-CPU build
window before `docker compose up`:

The host and path are `DEPLOY_HOST` (default `root@docker-host.example.com`)
and `DEPLOY_PATH` (`/docker/par-rt-db`) — both are `Makefile` variables;
override `DEPLOY_HOST` on the command line for a different host
(`make deploy DEPLOY_HOST=root@other-host`).

```sh
make deploy
```

The manual rsync path below is the same thing without the gate or the commit
label: it skips `checkall`, and unless you export `RTDB_BUILD_COMMIT` yourself,
`/healthz` reports `git_commit: "unknown"`. Use it only when you need to bypass
the gate intentionally. It mirrors the `Makefile`'s `deploy` target exactly:
rsync excludes everything `.gitignore` excludes (`--filter=':- .gitignore'`),
which already covers `target/`, `node_modules/`, and gitignored `.env*` files,
plus explicit `.git` and `.claude/` excludes. The `.git` rule protects both
ordinary checkout directories and linked-worktree pointer files. The `.claude/`
rule preserves the deploy host's agent metadata.

Source is synced to `/docker/par-rt-db` on the deploy host and built there (the
host is x86_64; do not `docker save` an arm64 image from a Mac).

```sh
# from the repo root on the workstation:
# Host reservation gate (see "Host reservation (quiet window)"): do not start
# this window while /var/tmp/par-host-reservation.json holds an unexpired
# lease — `make deploy` refuses; honor it here by the same convention.
# .gitignore already excludes target/, node_modules/, and .env/.env.* (which
# hold the live secrets on the deploy host) — --delete without that filter
# would wipe them out.
rsync -az --delete --filter=':- .gitignore' --exclude .git --exclude .claude/ \
  ./ root@docker-host.example.com:/docker/par-rt-db/
# on docker-host (the .env there holds the secrets, mode 600):
cd /docker/par-rt-db
BUILDER=par-rt-db-builder
if ! docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
  docker buildx create --name "$BUILDER" --driver docker-container \
    --driver-opt default-load=true \
    --driver-opt cpu-quota=400000 --driver-opt cpu-period=100000 \
    --buildkitd-config /docker/par-rt-db/deploy/buildkitd.toml
fi
docker buildx use "$BUILDER"
docker buildx inspect --bootstrap "$BUILDER" >/dev/null
BK=$(docker ps --format '{{.Names}}' | grep "^buildx_buildkit_${BUILDER}" | head -1)
# Two-CPU build window (see the deploy target comment above): constrain the
# builder container before the build, restore the four-CPU steady state after.
docker update --cpu-period 100000 --cpu-quota 200000 "$BK"
BUILDX_BUILDER="$BUILDER" docker compose up -d --build
docker compose ps
docker update --cpu-period 100000 --cpu-quota 400000 "$BK"
curl -fsS http://127.0.0.1:8300/healthz | jq .
# -> {"status":"ok","started_at":..,"uptime_seconds":..,"postgres":true}
# The build fingerprint ("version"/"git_commit"/"build_timestamp") is
# admin-only (SEC-129) — omitted from unauthenticated responses so an
# anonymous prober cannot pin the deployed version. To confirm the deployed
# commit, re-run with the admin bearer:
#   curl -fsS http://127.0.0.1:8300/healthz -H "Authorization: Bearer $RTDB_ADMIN_KEY" | jq .
# -> {"status":"ok","version":"<crate-version>","git_commit":"<sha>","build_timestamp":..,
#     "started_at":..,"uptime_seconds":..,"postgres":true}
# A 503 with "status":"degraded"/"postgres":false means the server is up but
# Postgres is not — `curl -f` exits non-zero so this surfaces in scripts.
```

Then verify the public path: `curl -fsS https://rtdb.example.com/healthz | jq .`.

## Host reservation (quiet window)

The deploy host also runs workloads that need guaranteed-CPU windows (e.g.
the-cube's 30-minute full-workload capacity benchmarks). Repeated DB image
builds interrupt those measurements, so both sides honor a cooperative
reservation file: while a reservation is active, new DB image builds and
`docker compose up` runs do not start.

**File**: `/var/tmp/par-host-reservation.json` on the deploy host. A single
JSON object:

```json
{"owner": "the-cube", "reason": "co-op capacity", "until": 1769100000}
```

`until` is a unix timestamp in seconds. The reservation is **active** while
`until > now`. Semantics are deliberate:

- **Absent** file → no reservation, deploy proceeds unchanged.
- **Expired** (`until <= now`) → no reservation, deploy proceeds unchanged.
- **Malformed/unreadable** (bad JSON, missing fields, a non-positive-integer
  `until`, or the file exists but can't be parsed) → the guard **fails
  closed**: it prints one line and exits nonzero. A lease someone tried to
  file must not be silently ignored. `jq` validates before anything is
  interpreted.
- **Active** → one line naming owner, reason, and expiry, exit nonzero.

`make host-reserve` writes the file atomically (`mktemp` + `mv`, mode 644 so
a different host user can read it), so the guard never reads a half-written
file. No environment variable is involved; this is a host-local file only.

**Targets** (plain POSIX sh + `jq`; run them on the deploy host from the
rsynced tree, or anywhere with `RESERVATION_FILE` overridden):

- `make host-reserve OWNER=the-cube REASON="co-op capacity" MINUTES=45` —
  create the lease. Input validation: `OWNER` must be non-empty
  `[A-Za-z0-9._-]`; `MINUTES` must be 1..1440; `REASON` must be non-empty.
- `make host-release` — remove the lease (idempotent; the owner can release
  early, or anyone after expiry).
- `make host-reservation-check` — status/dry-run: runs the exact deploy guard
  and exits nonzero while a lease is active. `RESERVATION_FILE=/tmp/fake.json`
  points it at a fake file — that is how the guard logic is exercised locally
  without touching the host.

**Enforcement**: `make deploy` (from the workstation) rsyncs the tree, then
the deploy host runs `make host-reservation-check` from that rsynced tree
before bootstrapping the builder and before any `docker compose up` — so no
image build or container restart starts while a lease is active. When
blocked, it prints one line naming owner, reason, and expiry and exits
nonzero without building or restarting anything.
`make deploy RESERVATION_OVERRIDE=1` skips the check for genuine emergencies.

**Adoption note (the-cube)**: this is an honor-system protocol — the guard
only gates this repo's own deploy/build entrypoints (`make deploy`; the
documented manual rsync path is an intentional bypass and must respect the
file by convention). the-cube files a reservation via `make host-reserve ...`
on the host before starting a benchmark window, and must equally honor the
file: no new benchmark window may start while an unexpired reservation (e.g.
from an emergency deploy) exists. Both sides read the same host-local file;
there is no other coordination channel.

## Topology: single instance by default; multi-instance is opt-in

**The default and recommended topology is a single process.** With
`RTDB_MULTI_INSTANCE=true` (ENH-022 Stages 1–4c), every formerly in-process
concern coordinates across replicas via Postgres LISTEN/NOTIFY, and a replica
fleet is safe behind one load balancer:

| State | Status with 2 replicas |
| --- | --- |
| OAuth login state | ✅ Fixed (ENH-022 Stage 1): `state` lives in the `rtdb_auth.oauth_states` table, so a login begun on one replica completes on another. |
| Op-feed (`/admin/stream`, dashboard live writes) | ✅ Fixed (ENH-022 Stage 2): fans out across replicas via Postgres `NOTIFY` on the `rtdb_ops` channel. |
| Presence (ENH-015 "who is online") | ✅ Fixed (ENH-022 Stage 3): per-room membership gossips via the `rtdb_presence` channel with liveness-beat eviction of dead replicas. |
| Rate-limit counters | ✅ Fixed (ENH-022 Stage 4): counters live in `rtdb_auth.rate_counters`; one shared ceiling per token/db/ip regardless of replica count. |
| Write ownership | ✅ Fixed (ENH-022 Stage 4 + 4c): one replica per database holds a Postgres advisory-lock lease and runs its committer; other replicas serve reads/subscriptions and FORWARD writes to the owner over NOTIFY (or take the lease on owner death — kill -9 releases the lock at the TCP session). |

The single-writer invariant is intact and must stay so: each database has
exactly one lease-holding committer, and correctness depends on that serialized
write path. Multi-instance means multiple *readers/connection-holders* plus
exactly one writer per database — non-owner writes are injected INTO the
owner's serialized turn (forwarding), never executed beside it.

When `RTDB_MULTI_INSTANCE=true`, also set `RTDB_INSTANCE_ID` to a stable,
distinct value per replica (e.g. `rtdb-a`, `rtdb-b`): NOTIFY self-dedupe works
with an auto-generated id, but a stable id makes logs and diagnostics easier to
correlate across replicas.

## Host machine authorization rollout

Host-scoped machine credentials (`hostCapability`, e.g.
`{"type":"parMeshV1","host":"macdaddy"}`; surface documented in the main
[README](../README.md#host-machine-authorization)) carry three operational
constraints:

1. **Legacy machine tokens bypass all row restrictions until you cut them
   over.** A token minted without a capability keeps its exact legacy
   unrestricted semantics — there is no automatic cutover, rotation, or
   retirement. Deploying this version enables the capability; moving consumers
   onto restricted credentials (mint with `hostCapability`, or enroll via a
   grant) and retiring the legacy tokens is an explicit operator action.
2. **All replicas must run this version before restricted credentials are
   issued.** Under `RTDB_MULTI_INSTANCE`, an older lease owner would deserialize
   a forwarded write's capability and ignore it, executing the write
   unrestricted. Upgrade every replica first; only then mint or accept
   restricted credentials.
3. **The mutation replay cache is now generation v2.** Idempotency identities
   encode the structured authorization identity (`[host_capability, tables,
   key]`) instead of the legacy raw key, so old cache rows are never read by
   this generation. Upgrade all replicas in one window and quiesce client
   retries during it, or accept possible one-time double-application of
   in-flight legacy retries across the upgrade (bounded by the dedup TTL,
   five minutes by default).

## Postgres image

The compose stack uses [`pgvector/pgvector:pg17`](https://hub.docker.com/r/pgvector/pgvector)
(not bare `postgres:17`) — required for vector search (#17), which depends on
the `pgvector` extension. The server creates the extension idempotently per
database (`CREATE EXTENSION IF NOT EXISTS vector` in `db::create_database` and
in `ddl::push_schema`), so the image is the only hard dependency. Vector search
has been live in prod since 2026-07-25 (vector 0.8.5); do not downgrade this
image to bare `postgres:17`, or vector indexes will fail to compile on schema
push.

## Collation

The cluster initializes with a deterministic, versionless collation
(`POSTGRES_INITDB_ARGS: "--lc-collate=C --lc-ctype=C.UTF-8"` in the compose files):
`C` collate carries no libc version, so it cannot trigger the
collation-version-mismatch warning that `en_US.utf8` raises whenever the image's
glibc changes. ctype is `C.UTF-8` for UTF-8 charset handling. This applies to a
**fresh** (empty) data volume. **Prod was migrated to `C` on 2026-07-25**: the
original `en_US.utf8` `rtdb-pg` volume was `pg_dump`ed, wiped
(`docker compose down -v`), re-initialized, and `pg_restore`d — every schema
(`kanban`, `hackzors`, `kanban_dev`, `vecsmoke`, `rtdb_auth`) round-tripped with
matching row counts. To migrate any other existing `en_US.utf8` volume to `C`:
`pg_dump -Fc` the `rtdb` database, `docker compose down -v`, `up`, `pg_restore`
(downtime; keep the verified dump + a volume tarball as safety nets).

**`C` is load-bearing beyond the version warning.** Subscription invalidation
compares text range bounds and sort boundaries byte-wise in Rust
(`server/src/subs.rs::cmp_binds`), which matches Postgres only under a `C`
collation. On a linguistic collation (`en_US.utf8`) Postgres would order some
text differently, and a text bound could judge an inside-the-window document to
be outside — a dropped realtime update. Equality is byte-wise under any
deterministic collation, so eq-prefix matching is safe either way; only ORDER
comparisons are exposed. If a cluster must run non-`C`, enable
`RTDB_SUBS_VERIFY_SKIP_EVERY` and watch `rtdb_subs_missed_pushes_total`.

## Subscription-invalidation observability

`fan_out` skips re-running subscriptions it can prove a write didn't affect. A
wrong skip is silent (the subscriber just never hears about the change), so two
metrics exist for it on `/metrics` and `GET /admin/metrics`:

- `rtdb_subs_skips_total{class="point|indexed|ordered"}` + `rtdb_subs_reruns_total`
  — effectiveness. Always on, no cost. Shown on the dashboard metrics page.
- `rtdb_subs_missed_pushes_total` — **alert on any increase.** Non-zero means
  invalidation under-approximated. Only populated when verification is on.

`RTDB_SUBS_VERIFY_SKIP_EVERY=N` shadow-verifies 1 skip in every N: the query
runs anyway and its result is compared against the last pushed one. **It ships
on at 1000** (`.env.example` and compose agree) — set `0` to disable it, lower
it to tune (every verification costs the Postgres round-trip the skip avoided).
**Setting it in `.env` is not enough on its own** — compose's
`environment:` block is an explicit allowlist, so a new `RTDB_*` key must also
be forwarded there (this one is). After changing it, recreate the
container (`docker compose up -d server`) and confirm
`rtdb_subs_skip_verifications_total` starts climbing; if it stays 0 while skips
accumulate, the variable isn't reaching the process. A divergence logs at ERROR, increments the counter, and pushes the
corrected result (so it repairs, not just reports). Each verification costs the
Postgres round-trip the skip avoided. **Recommendation: keep a permanent
standing canary on — e.g. `RTDB_SUBS_VERIFY_SKIP_EVERY=200`** (prod has run
that setting since 2026-07-30). A skipped update is silent, so the verifier
stays valuable as a detector rather than being toggled off.
After changing invalidation logic, temporarily lower it to N=20 for a few days
and confirm `rtdb_subs_missed_pushes_total` stays 0, then return it to 200.

### Monitoring the invalidation fan-out

Beyond the missed-push alert, watch the **rerun ratio** — the share of fan-out
decisions that ended in a full table-level re-run rather than a provable skip.
Subscription re-runs execute inside the committer turn, so a database whose
re-runs dominate is one whose writes queue behind its own subscriber load
(`distinct`/`search`/`vector`/`hybrid` subscriptions — and an unwindowed
`aggregate` with no eq/range bound — stay table-level and re-run on every
write to their table; a windowed `aggregate` skips like `count`/`collect` —
see `docs/ARCHITECTURE.md`). The ratio over `/metrics`:

```promql
rate(rtdb_subs_reruns_total[5m])
  / (rate(rtdb_subs_reruns_total[5m]) + sum(rate(rtdb_subs_skips_total[5m])))
```

(`rtdb_subs_skips_total` carries a `class` label — point/indexed/ordered — so
`sum()` collapses it to match the unlabeled rerun counter; both are
instance-wide aggregates, deliberately without per-db labels to keep `/metrics`
cardinality bounded.) A value near 0 means invalidation is proving most writes
irrelevant; a sustained value above ~0.5 means re-runs dominate and writes on
that instance are paying for subscriber load. Suggested alert: **ratio > 0.5
sustained for 15m** — a capacity signal, not a correctness defect (that's
`rtdb_subs_missed_pushes_total` above).

To find *which* database is rerun-heavy (the Prometheus ratio is instance-wide),
check the per-db `rerunRatio` — `perDb[]` on `GET /admin/subscriptions` and
`perDbSubs[]` on `GET /admin/metrics`, behind the admin key (ENH-024). Each row
carries `reruns`, `skips` (the class total), and `rerunRatio` in [0, 1]; the
dashboard's Subscriptions page renders the same ratio per database and marks
anything above 0.5 in amber. Remediation levers, in order of effectiveness:

- **Narrow the subscription** — give the query an index/range read set
  (`withIndex`/`eq`/`take` bounds) instead of a table-level
  collect/count/aggregate, so writes outside the window are provably skipped.
- **Split hot tables** — move the rows a table-level subscription doesn't need
  (or the ones it does) into a separate table, so each write fans out to fewer
  subscriptions.
- **Quota caps** — `maxSubsPerDb` (via `PATCH /admin/config`; one of the eight
  hot fields, alongside `maxTablesPerDb` and `maxStorageBytesPerDb`) bounds how
  many subscriptions a database can hold, capping the worst-case fan-out per
  committer turn.

## Monitoring

`GET /metrics` is the Prometheus scrape endpoint — plain text exposition,
aggregate-only (no per-db, no principal data), same auth posture as `/healthz`
(none). Content-negotiated on `Accept`: a browser (`text/html`) is served the
SPA's `index.html` when `RTDB_STATIC_DIR` is set; everything else (Prometheus
sends `application/openmetrics-text`, curl, API-only deploys) gets the
Prometheus text. Point a scraper at `https://rtdb.example.com/metrics`.

The subscription-invalidation canary above is the one alert to wire up: alert
on any increase of `rtdb_subs_missed_pushes_total` (only populated when
verification is on — prod runs `RTDB_SUBS_VERIFY_SKIP_EVERY=200`). The admin
JSON snapshot with per-db breakdowns (storage, subs, quota rejections) stays at
`GET /admin/metrics`, behind the admin key — do not scrape that from Prometheus,
since per-db labels would blow up cardinality.

Slow queries are a separate operator surface: `RTDB_SLOW_QUERY_MS` (default `0`
= off) thresholds what counts as slow, `RTDB_SLOW_QUERY_CAPACITY` bounds the
in-memory ring (default 200), and the captured rows surface via the admin API /
dashboard. `RTDB_SLOW_QUERY_LOG_PARAMS=true` also records the query's bound
parameter values — a privacy tradeoff, since params can contain user content;
it defaults to `false`. See `.env.example` for the full set.

## Tracing (OpenTelemetry / OTLP, ENH-018)

`/metrics` answers "how much"; OTLP tracing answers "why was *this* request
slow." It is opt-in at two layers: the `otel` cargo feature must be compiled
in, and `RTDB_OTEL_ENABLED=true` must be set at runtime. The shipped
`Dockerfile` builds `--features otel`, so docker deploys carry the exporter;
source builds need the feature flag. `RTDB_OTEL_ENABLED` (default `false`)
still gates the layer at runtime, so the default deploy makes zero OTLP calls.
`docker-compose.yml` additionally forwards the standard
`OTEL_EXPORTER_OTLP_HEADERS` var — the opentelemetry-otlp crate reads it
natively (comma-separated `key=value` pairs, values URL-decoded) — which is
how a bearer-gated collector receives `authorization=Bearer <token>`:

| Var | Default | Meaning |
|---|---|---|
| `RTDB_OTEL_ENABLED` | `false` | Runtime master switch — `false` produces zero OTLP network calls even in a feature-compiled binary. |
| `RTDB_OTEL_ENDPOINT` | `http://127.0.0.1:4317` | OTLP/gRPC collector endpoint (4317 is the standard port). |
| `RTDB_OTEL_SERVICE_NAME` | `par-rt-db` | `service.name` resource attribute on every span. |
| `RTDB_OTEL_SAMPLE_RATIO` | `0.05` | Head sampler ratio in `[0.0, 1.0]`; a malformed value fails boot. |
| `OTEL_EXPORTER_OTLP_HEADERS` | *(empty)* | Standard OTLP headers var, read by the crate, not the server. Bearer-gated collectors need `authorization=Bearer <token>`. |

Point `RTDB_OTEL_ENDPOINT` at a collector (e.g. an `otel/opentelemetry-collector`
sidecar or a hosted backend). The spans worth alerting on:

- **`committer.mutate`** — carries `queue_wait_ms`, the gap between enqueue and
  dequeue in the per-db serialized queue. This is the single most useful
  per-request latency signal and the one the architecture otherwise makes
  invisible; a sustained non-zero value means writes are queuing behind the
  single writer for that database.
- **`subs.fan_out` / `subs.rerun`** — a slow subscription's re-run query blocks
  all writes to that database (it runs inside the committer turn). Per-sub
  `subs.rerun` spans (carrying `table`/`terminal`) make the offender obvious.
- **`query.execute` / `txn.execute`** — splits "the DSL is slow" vs "Postgres
  is slow".

Span attributes are bounded (`db`, `table`, `terminal`, `steps`,
`queue_wait_ms`) — never document ids, no user ids, no content — so cardinality
stays safe under the head sampler. The exporter flushes on SIGTERM, so a
`compose down` does not drop the last in-flight batch.

### Reaching the shared collector (deploy-site wiring)

A collector run by another compose project (this deployment exports to the
observability stack's LGTM collector on the same host) is reachable only after
the server container joins that stack's Docker network. That is deployment-site
topology, not repo config: compose has no optional-external-network syntax
(`required` on service networks is rejected by current compose), so the attach
lives in a host-local `docker-compose.override.yml` next to the tracked
compose file. Because the file name is gitignored, `make deploy`'s
`rsync --delete --filter=':- .gitignore'` protects it from deletion and the
CI path's `git checkout -f` leaves it alone — the attach survives both deploy
paths and every container recreation. Shape (network name and container DNS
name are deployment-site values):

```yaml
services:
  server:
    networks:
      default:
      otel-collector:
networks:
  otel-collector:
    external: true
    name: <collector-stack-network>
```

With the attach in place the collector is reachable at its container DNS name
on that network, e.g. `RTDB_OTEL_ENDPOINT=http://<collector-container>:4317`.
The bearer token itself ships in the deploy-host `.env`
(`OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer <token>`; the deploy
`.env.example` does not carry the real token).

### Enablement checklist

1. Deploy-host `.env`: set `RTDB_OTEL_ENABLED=true`, `RTDB_OTEL_ENDPOINT` to
   the collector's container DNS name on the shared network, and
   `OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer <token>`.
2. Install the `docker-compose.override.yml` network attach (shape above).
3. Deploy. On the next `compose up` the container is created with the network
   attached and otel on.
4. Verify: `docker exec` into the server container, resolve the collector
   name and TCP-connect to 4317; then confirm spans arrive (the collector
   backend should show `service.name=par-rt-db`).

### Enablement checklist (manual/local)

For a local collector: no override needed — set the four `RTDB_OTEL_*` vars,
point `RTDB_OTEL_ENDPOINT` at the local collector's published loopback port,
and `docker compose up -d` (the compose file itself has no external-network
dependency).

## Secrets (`/docker/par-rt-db/.env`, not committed)

This section covers the secrets you must provision before first boot. The
canonical reference for **every** `RTDB_*` variable (with defaults and
commentary) is [`/.env.example`](../.env.example) — this file names only the
operator-critical subset.

- `POSTGRES_PASSWORD`, `RTDB_ADMIN_KEY` — `openssl rand -hex 32`. `RTDB_ADMIN_KEY`
  is also stored in parvault for admin CLI use.
- `RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET` — from parvault
  (`RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET`).
- `RTDB_GOOGLE_CLIENT_ID` / `RTDB_GOOGLE_CLIENT_SECRET` — from parvault
  (`RTDB_GOOGLE_CLIENT_ID` / `RTDB_GOOGLE_CLIENT_SECRET`); optional, leave blank
  to disable Google login. **Both must be passed to the server in
  `docker-compose.yml`'s `environment:` block** (they are, alongside the GitHub
  pair) — the server reads them at boot, so a change needs `docker compose up -d`
  to take effect. Provider setup (callbacks, scopes, env vars for all six
  providers) is documented in [`docs/OAUTH_SETUP.md`](../docs/OAUTH_SETUP.md).
- `RTDB_AUTH_ANONYMOUS_ENABLED` (default `false`) is the server-wide gate for
  anonymous login; when it is on, each database still opts in individually via
  `GET|PATCH /admin/db/{db}/anonymous-access` (SEC-103).
- `RTDB_ALLOWED_ORIGINS` — the SPA origin(s). This is a hot setting: after
  first boot, change it with
  `PATCH /admin/config` (body `{ "allowedOrigins": ["https://spa.example.com"] }`)
  or the dashboard Settings page — the value is persisted in the `rtdb_config`
  row and overlaid onto env on every subsequent boot, so editing
  `RTDB_ALLOWED_ORIGINS` in `.env` and restarting has no effect once a PATCH
  has ever been applied. See [Hot config beats env](#hot-config-beats-env).
- `RTDB_BUILD_COMMIT` (optional) — git short sha baked into `/healthz`. Set it
  to the deployed commit before `BUILDX_BUILDER=par-rt-db-builder docker
  compose up -d --build`, e.g. `RTDB_BUILD_COMMIT=$(git rev-parse --short HEAD)`
  (run on the workstation that has `.git`, before rsync). If unset, `/healthz`
  reports `git_commit: "unknown"`.
- `RTDB_TRUSTED_PROXY` (default `false`; `docker-compose.yml` and
  `.env.example` set it `true`) — **required** behind the Cloudflare tunnel.
  When `true`, the server trusts the tunnel's forwarding headers
  (`CF-Connecting-IP` / `X-Forwarded-For` for the per-IP rate-limit key,
  `X-Forwarded-Proto` for cookie `Secure`/HSTS). Every request to
  `docker-host:8300` arrives from the local `cloudflared` process, not the
  original client, so with this `false` every request would key off the same
  peer address — collapsing per-IP rate limits onto one bucket and breaking
  `Secure`-cookie detection. Leave it `false` only for a deploy directly
  reachable without a trusted proxy in front (SEC-201).
- `RTDB_COOKIE_SECURE` (default `true`) — sets the `Secure` attribute on every
  session/CSRF cookie unconditionally. Only set it `false` for local HTTP
  development (e.g. `http://localhost`); never disable it on a public deploy.

## Dashboard / SPA

The operator dashboard (React SPA from `dashboard/`) is **baked into the server
image**, not a live-mounted volume: the `dashboard` stage in `Dockerfile` runs
the bun/vite build and copies `dist/` to `/app/dashboard-dist`, and
`RTDB_STATIC_DIR=/app/dashboard-dist` tells the server to serve it same-origin
(as the router's last-resort SPA fallback, so it never shadows `/healthz`,
`/api/*`, `/admin/*`, `/sync`, or `/auth/*`). Consequences:

- A **frontend-only change ships via the standard
  `BUILDX_BUILDER=par-rt-db-builder docker compose up -d --build`** (image
  rebuild + server container recreate) — it is NOT a hot volume you can swap
  without a rebuild.
- Same-origin serving means the dashboard needs **no `RTDB_ALLOWED_ORIGINS`
  entry** of its own.
- Unset/empty `RTDB_STATIC_DIR` (or a missing dir) ⇒ API-only, no SPA served.

## Admin bootstrap (after first deploy)

```sh
# create a database, push its schema, mint a machine token, allowlist a user:
curl -s -X POST https://rtdb.example.com/admin/create-db \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"name":"kanban"}'
curl -s -X POST https://rtdb.example.com/admin/push-schema \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"db":"kanban","schema":{...}}'
curl -s -X POST https://rtdb.example.com/admin/mint-token \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"db":"kanban","name":"cli"}'
curl -s -X POST https://rtdb.example.com/admin/allowlist \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"db":"kanban","action":"add","email":"you@example.com"}'
```

## Hot config beats env

Eight settings (`allowedOrigins`, `sessionTtlDays`, `maxFileSize`,
`idempotencyTtlMs`, `maxTablesPerDb`, `maxStorageBytesPerDb`, `maxSubsPerDb`,
`changeLogMaxRows` — the `HotConfig` fields) are runtime-mutable and
**persisted**: the first `PATCH /admin/config` writes the merged row into the
single-row `rtdb_config` table, and every boot thereafter loads that row and
overlays it onto the env-seeded defaults field by field
(`config/hot.rs` `load_hot` / `PersistedHotConfig::merge_onto`). After that
first PATCH, editing the corresponding `RTDB_*` variables in `.env` and
restarting has **no effect** — the persisted field wins.

Change these settings through `PATCH /admin/config` or the dashboard Settings
page (both validate and persist). Example:

```sh
curl -X PATCH "$RTDB_URL/admin/config" \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"allowedOrigins": ["https://spa.example.com"], "maxSubsPerDb": 500}'
```

To fall back to env for one field, PATCH it to the value you want env to
carry (there is no "unset to env" — the row stores every field). `0` on a
quota cap means unlimited.

## Backups & restore

The server ships a full backup lifecycle: scheduled `pg_dump`, a manual async
trigger, list/download/delete, and restore into a fresh database. The operator
console surfaces all of it; the same routes are available directly as admin API
calls and are mirrored in the ts/rust/python admin clients.

### Scheduled backups

Enable the built-in managed backup loop with `RTDB_BACKUP_ENABLED=true`
(default `false`) plus `RTDB_BACKUP_CRON` (5-field UTC cron, default
`0 3 * * *`), `RTDB_BACKUP_DIR` (Compose default `/backups`), and
`RTDB_BACKUP_RETENTION` (count, default `7`). The canonical Compose file mounts
`/backups` from the persistent `rtdb-backups` named volume. Keep
`RTDB_BACKUP_DIR=/backups` for retained backups; changing it requires mounting
another persistent path. The generic server and `.env.example` default remains
`./backups`, which resolves to `/backups` in the image's default `/` working
directory. See `BackupConfig::from_env` in `server/src/config/backup.rs` and
`.env.example` for the full semantics and reference values. The Docker image
creates `/backups` owned by UID 10001 with mode 0700 before switching to the
non-root server user, so a newly created Compose volume is writable. The image
installs the pinned PostgreSQL 17 client (`postgresql-client-17`) so its
`pg_dump` major version matches the `pgvector/pgvector:pg17` database image,
and `GET /admin/backups` lists the dumps. PostgreSQL data persists separately
in the `rtdb-pg` named volume.

### Manual trigger, download, delete

- `POST /admin/backup` → `202 Accepted` — spawns one `pg_dump` **outside the
  committer** (a read; same as the cron task). Gated by an in-progress flag: a
  second call while a dump is running returns `409`.
- `GET /admin/backups` — lists existing dumps.
- `GET /admin/backups/{name}` — downloads a dump.
- `DELETE /admin/backups/{name}` → `204` — deletes a dump.

All name-bearing routes pass a path-traversal guard (`backup::validate_dump_name`).

### Restore

`POST /admin/restore` (typed body: `confirm == "<dump-name>"`) restores a dump
into a **fresh `rtdb_restored_<stamp>` Postgres database** via `createdb` +
`pg_restore --no-owner --no-privileges`. The live `rtdb` database is never
touched, so there are no locks or races with the writer.

**`CREATEDB` privilege is required on the DB role** (new — the server previously
only ran `CREATE SCHEMA`/`CREATE EXTENSION`). For the docker-compose deploy the
`POSTGRES_USER` role is a superuser and already has it; on a managed Postgres,
grant it explicitly (`ALTER ROLE "<role>" CREATEDB;`).

### Cutover runbook

Restore produces a verified `rtdb_restored_<stamp>` database alongside the live
`rtdb` one. To cut over:

1. Trigger the restore from the console (or `POST /admin/restore` with
   `confirm == "<name>"`); verify row counts in the restored DB.
2. Edit the `RTDB_DATABASE_URL:` line in `docker-compose.yml`'s `environment:`
   block to point at `rtdb_restored_<stamp>` — the URL is hardcoded in that
   block (only `${POSTGRES_PASSWORD}` is interpolated), so setting it in
   `.env` has no effect.
3. Restart the server (`docker compose up -d server`).
4. Verify the cutover: `curl -fsS localhost:8300/healthz` (or the tunnel URL)
   reports `"status":"ok"`, and one query against a row known to exist only
   in the restored data returns it.
5. Once stable, drop the old database at your discretion.

Credentials for `createdb`/`pg_restore` travel via the `PG*` env vars, never on
the argv.

## Troubleshooting

Common operator symptoms on the live deploy:

- **`/healthz` returns 503 with `"status":"degraded"` / `"postgres":false`** —
  the server process is up but cannot reach Postgres. Check
  `docker compose ps`, the `rtdb-pg` container health, and the
  `RTDB_DATABASE_URL` line in `docker-compose.yml`'s `environment:` block.
  `curl -f` exits non-zero on the 503, so this
  surfaces in deploy/wrapper scripts.
- **A new `RTDB_*` var set in `.env` has no effect.** Compose's `environment:`
  block is an explicit allowlist — a key only present in `.env` is never passed
  to the container. Add it to `docker-compose.yml`'s `environment:` list too,
  then `docker compose up -d server`. `make env-drift-check` (first stage of
  `checkall`) catches this locally.
- **`POST /admin/restore` fails with a permission/`createdb` error.** Restore
  creates a fresh `rtdb_restored_<stamp>` database via `createdb`, so the DB
  role needs the `CREATEDB` privilege. The docker-compose `POSTGRES_USER` is a
  superuser and already has it; on managed Postgres, run
  `ALTER ROLE "<role>" CREATEDB;`.
- **The image build fails with `can't find '<name>' test at ...` during
  `make deploy`.** The Dockerfile's dependency-caching layer builds
  `rtdb-server` against stubbed sources, and Cargo validates every `[[test]]`
  target declared in a workspace member's `Cargo.toml` (except the member
  actually being built) at manifest-parse time — even one it never runs. A
  new `[[test]]` entry in, for example, `rust-client/Cargo.toml` needs a
  matching placeholder `touch <path>` in the Dockerfile's dependency layer, or
  the build fails on the deploy host, long after `make checkall` looked clean.
  `scripts/dockerfile-stub-check.sh` (wired into `make checkall` as the
  `dockerfile-stub-check` target) catches this locally: it diffs every
  declared `[[test]]` path against the `.rs` paths the Dockerfile's `touch`
  stub list names and fails with the exact missing paths when one is absent.
  Run it directly with:

  ```sh
  ./scripts/dockerfile-stub-check.sh
  ```

  Its failure output lists each missing `<member>/tests/<name>.rs` path —
  add each one to the `touch` stub list in the Dockerfile's dependency layer
  and re-run the check.
- **An env change to a hot setting has no effect after a restart.** See
  [Hot config beats env](#hot-config-beats-env): once any `PATCH /admin/config`
  has been applied, the persisted `rtdb_config` row overlays env on every boot.
  Change the value via `PATCH /admin/config` or the dashboard Settings page.
- **Boot fails with "RTDB_ADMIN_KEY is set to an obvious placeholder" (or the
  min-length error).** SEC-110's boot gate rejects an empty key, a key under
  16 characters, and a denylist of copy-paste placeholders
  (`changeme`, `admin`, `test-key`, …). Set a real random key (e.g. 64 hex
  chars) in the deploy `.env`.
- **Boot fails on a malformed numeric env value.** Every `RTDB_*` knob parsed
  as a number (timeouts, caps, intervals) aborts startup naming the offending
  variable — fix the value in `.env`; the server does not silently fall back
  to the default.
- **Dashboard shows the old SPA after a frontend change.** The SPA is baked into
  the server image, not a live-mounted volume — a frontend change ships only via
  `BUILDX_BUILDER=par-rt-db-builder docker compose up -d --build` (image rebuild
  + container recreate). A plain `docker compose up -d` without `--build` keeps
  the old image and the old SPA. A hard reload (`Cmd/Ctrl`+`Shift`+`R`) clears a
  stale browser cache, but the image-rebuild step is the real fix.

## Rollback

There are no image tags to pin: `make deploy` rsyncs the current checkout to
docker-host and builds in place, so rolling back a bad deploy means redeploying an
older commit.

1. On the workstation, from the repo root, check out the last-known-good commit
   and redeploy it:

   ```sh
   git checkout <last-known-good-commit>
   make deploy
   ```

  `make deploy` re-runs the gate, rsyncs that commit's source over
  `/docker/par-rt-db` (the `.env` excludes keep the live secrets intact), and
  rebuilds the image on the host with the capped
  `par-rt-db-builder` BuildKit builder.

2. Verify: `curl -fsS https://rtdb.example.com/healthz -H "Authorization: Bearer $RTDB_ADMIN_KEY" | jq .`
   reports the expected `git_commit` (the redeployed sha; the fingerprint is
   admin-only — SEC-129), and a spot query against a row
   you know was affected by the bad deploy behaves correctly again.

3. Return the workstation to the deploy branch (`git checkout main`) so the
   next deploy doesn't silently re-ship the old commit.

`docker compose down` only stops the stack (the named volume `rtdb-pg` persists
data) — it is not a rollback. To wipe data too: `docker compose down -v`.

### Downgrading across session revocation (b5c27c3)

The session-revocation schema is additive (`session_id`, `revoke_secret_hash`,
`revoked_at` on `rtdb_auth.sessions`), so an OLD binary happily runs against the
upgraded database — but it cannot see `revoked_at`. The pre-revocation resolvers
matched `WHERE token_hash = $1` with no revocation filter, so a REVOKED session
left as a tombstone still authenticates under an old binary, and an old logout
hard-deletes the tombstone (destroying the capability anchor). Rolling back to a
pre-`b5c27c3` commit therefore resurrects revoked sessions until their TTL
expires. The pinned probe is `sessions_test::old_resolver_sql_resolves_revoked_tombstones_downgrade_hazard`.

Before rolling back across `b5c27c3`, purge the tombstones on the live database
(revoked sessions become plain deletions — their capability anchors are moot on
a binary that cannot answer them anyway):

```sql
DELETE FROM rtdb_auth.sessions WHERE revoked_at IS NOT NULL;
```

**This purge is an owner-authorization step, not a routine command**: it deletes
credential rows on the live database. Run it only with the operator's explicit
go-ahead for the specific rollback, during a write-quiet window (no concurrent
tombstone creation), and never as a standing cleanup — a co-op consumer holding
sessions on the instance must be consulted first. The pinned probe covers the
old resolver SQL shape, not a runtime old-binary rollback; mixed-version runtime
verification remains open. (Newer binaries are unaffected either way: every
resolver excludes `revoked_at IS NOT NULL`.)
