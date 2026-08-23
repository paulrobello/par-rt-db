# Deploying par-rt-db to a Docker host

The target is a standalone Docker host (plain `docker compose`, not Swarm). Public
traffic reaches it through the host `cloudflared` tunnel, which routes
`rtdb.example.com` -> `http://localhost:8300`. No ports 80/443 are opened on the
VPS and no reverse proxy is needed — TLS is terminated at Cloudflare's edge.

## Table of contents

- [One-time DNS/tunnel wiring (already done 2026-07-21)](#one-time-dnstunnel-wiring)
- [Deploy / update](#deploy--update)
- [Topology: single instance by default; multi-instance is opt-in](#topology-single-instance-by-default-multi-instance-is-opt-in)
- [Postgres image](#postgres-image)
- [Collation](#collation)
- [Subscription-invalidation observability](#subscription-invalidation-observability)
- [Monitoring](#monitoring)
- [Tracing (OpenTelemetry / OTLP, ENH-018)](#tracing-opentelemetry--otel-enh-018)
- [Secrets (`/docker/par-rt-db/.env`, not committed)](#secrets-dockerpar-rt-dbenv-not-committed)
- [Dashboard / SPA](#dashboard--spa)
- [Admin bootstrap (after first deploy)](#admin-bootstrap-after-first-deploy)
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

The preferred path is `make deploy` from the repo root: it runs `make checkall`
first (the full gate), then rsyncs to docker-host and runs `docker compose up -d
--build` with `RTDB_BUILD_COMMIT` baked in (so `/healthz` reports the deployed
commit). See the [`Makefile`](../Makefile) `deploy` target for the canonical
commands.

```sh
make deploy
```

The manual rsync path below is the same thing without the gate or the commit
label: it skips `checkall`, and unless you export `RTDB_BUILD_COMMIT` yourself,
`/healthz` reports `git_commit: "unknown"`. Use it only when you need to bypass
the gate intentionally.

Source is synced to `/docker/par-rt-db` on docker-host and built there (the host is
x86_64; do not `docker save` an arm64 image from a Mac).

```sh
# from the repo root on the workstation:
# .env/.env.* MUST be excluded — they are gitignored (don't exist in the
# local checkout) but hold the live secrets on docker-host, so `--delete` without
# these excludes would wipe them out.
rsync -az --delete \
  --exclude target/ --exclude .git/ --exclude .superpowers/ --exclude node_modules/ \
  --exclude .env --exclude '.env.*' \
  ./ root@docker-host.example.com:/docker/par-rt-db/

# on docker-host (the .env there holds the secrets, mode 600):
cd /docker/par-rt-db
docker compose up -d --build
docker compose ps
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
enabled at 1000** (`DEFAULT_SUBS_VERIFY_SKIP_EVERY` in `server/src/config.rs`;
`.env.example` and compose agree) — set `0` to disable it.
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
(`distinct`/`aggregate`/`search`/`vector` subscriptions stay table-level and
re-run on every write to their table — see `docs/ARCHITECTURE.md`). The ratio
over `/metrics`:

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
- **Quota caps** — `max_subs_per_db` (via `PATCH /admin/config`) bounds how
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
slow." It is opt-in at two layers: the docker image must be built with the
`otel` cargo feature, and `RTDB_OTEL_ENABLED=true` must be set at runtime. The
shipped `Dockerfile` does **not** build with `--features otel` by default (the
default build carries no OTel code and is byte-compatible with tracing off), so
enabling tracing is a `Dockerfile` one-line change (`cargo build --features
otel --release`) plus the four `RTDB_OTEL_*` vars already forwarded by
`docker-compose.yml`:

| Var | Default | Meaning |
|---|---|---|
| `RTDB_OTEL_ENABLED` | `false` | Runtime master switch — `false` produces zero OTLP network calls even in a feature-compiled binary. |
| `RTDB_OTEL_ENDPOINT` | `http://127.0.0.1:4317` | OTLP/gRPC collector endpoint (4317 is the standard port). |
| `RTDB_OTEL_SERVICE_NAME` | `par-rt-db` | `service.name` resource attribute on every span. |
| `RTDB_OTEL_SAMPLE_RATIO` | `0.05` | Head sampler ratio in `[0.0, 1.0]`; a malformed value fails boot. |

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
`queue_wait_ms`) — never document ids, user ids, or content — so cardinality
stays safe under the head sampler. The exporter flushes on SIGTERM, so a
`compose down` does not drop the last in-flight batch.

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
- `RTDB_ALLOWED_ORIGINS` — the SPA origin(s); adjust when the client's final
  origin is known, then `docker compose up -d` to apply.
- `RTDB_BUILD_COMMIT` (optional) — git short sha baked into `/healthz`. Set it
  to the deployed commit before `docker compose up -d --build`, e.g.
  `RTDB_BUILD_COMMIT=$(git rev-parse --short HEAD)` (run on the workstation
  that has `.git`, before rsync). If unset, `/healthz` reports
  `git_commit: "unknown"`.

## Dashboard / SPA

The operator dashboard (React SPA from `dashboard/`) is **baked into the server
image**, not a live-mounted volume: the `dashboard` stage in `Dockerfile` runs
the bun/vite build and copies `dist/` to `/app/dashboard-dist`, and
`RTDB_STATIC_DIR=/app/dashboard-dist` tells the server to serve it same-origin
(as the router's last-resort SPA fallback, so it never shadows `/healthz`,
`/api/*`, `/admin/*`, `/sync`, or `/auth/*`). Consequences:

- A **frontend-only change ships via the standard `docker compose up -d --build`**
  (image rebuild + server container recreate) — it is NOT a hot volume you can
  swap without a rebuild.
- Same-origin serving means the dashboard needs **no `RTDB_ALLOWED_ORIGINS`
  entry** of its own.
- Unset/empty `RTDB_STATIC_DIR` (or a missing dir) ⇒ API-only, no SPA served.

## Admin bootstrap (after first deploy)

```sh
# create a database, push its schema, mint a machine token, allowlist a user:
curl -s -X POST https://rtdb.example.com/admin/create-db \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -d '{"name":"kanban"}'
curl -s -X POST https://rtdb.example.com/admin/push-schema \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -d '{"db":"kanban","schema":{...}}'
curl -s -X POST https://rtdb.example.com/admin/mint-token \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -d '{"db":"kanban","name":"cli"}'
curl -s -X POST https://rtdb.example.com/admin/allowlist \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -d '{"db":"kanban","action":"add","email":"you@example.com"}'
```

## Backups & restore

The server ships a full backup lifecycle: scheduled `pg_dump`, a manual async
trigger, list/download/delete, and restore into a fresh database. The operator
console surfaces all of it; the same routes are available directly as admin API
calls and are mirrored in the ts/rust/python admin clients.

### Scheduled backups

Enable the built-in managed backup loop with `RTDB_BACKUP_ENABLED=true` plus
`RTDB_BACKUP_CRON` (5-field UTC cron, default daily 03:00), `RTDB_BACKUP_DIR`,
and `RTDB_BACKUP_RETENTION` (count, default 7) — the Deployment section of
`CLAUDE.md` has the full semantics. The docker image installs `postgresql-client`
so `pg_dump` is present, and `GET /admin/backups` lists the dumps. Data also
persists in the `rtdb-pg` named volume.

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
- **Dashboard shows the old SPA after a frontend change.** The SPA is baked into
  the server image, not a live-mounted volume — a frontend change ships only via
  `docker compose up -d --build` (image rebuild + container recreate). A plain
  `docker compose up -d` without `--build` keeps the old image and the old SPA.
  A hard reload (`Cmd/Ctrl`+`Shift`+`R`) clears a stale browser cache, but the
  image-rebuild step is the real fix.

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
   rebuilds the image on the host (`docker compose up -d --build`).

2. Verify: `curl -fsS https://rtdb.example.com/healthz -H "Authorization: Bearer $RTDB_ADMIN_KEY" | jq .`
   reports the expected `git_commit` (the redeployed sha; the fingerprint is
   admin-only — SEC-129), and a spot query against a row
   you know was affected by the bad deploy behaves correctly again.

3. Return the workstation to the deploy branch (`git checkout main`) so the
   next deploy doesn't silently re-ship the old commit.

`docker compose down` only stops the stack (the named volume `rtdb-pg` persists
data) — it is not a rollback. To wipe data too: `docker compose down -v`.
