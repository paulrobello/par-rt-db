# Deploying par-rt-db to lenny2

lenny2 is a standalone Docker host (plain `docker compose`, not Swarm). Public
traffic reaches it through the host `cloudflared` tunnel, which routes
`rtdb.pardev.net` -> `http://localhost:8300`. No ports 80/443 are opened on the
VPS and no reverse proxy is needed — TLS is terminated at Cloudflare's edge.

## Table of contents

- [One-time DNS/tunnel wiring (already done 2026-07-21)](#one-time-dnstunnel-wiring-already-done-2026-07-21)
- [Deploy / update](#deploy--update)
- [Postgres image](#postgres-image)
- [Collation](#collation)
- [Subscription-invalidation observability](#subscription-invalidation-observability)
- [Monitoring](#monitoring)
- [Secrets (`/docker/par-rt-db/.env`, not committed)](#secrets-dockerpar-rt-dbenv-not-committed)
- [Dashboard / SPA](#dashboard--spa)
- [Admin bootstrap (after first deploy)](#admin-bootstrap-after-first-deploy)
- [Backups & restore](#backups--restore)
- [Troubleshooting](#troubleshooting)
- [Rollback](#rollback)

## One-time DNS/tunnel wiring (already done 2026-07-21)

- Proxied CNAME `rtdb.pardev.net` -> `<lenny2-tunnel-id>.cfargotunnel.com`
  (overrides the `*.pardev.net` wildcard that points at lenny1).
- Tunnel ingress rule `rtdb.pardev.net` -> `http://localhost:8300` appended to
  the lenny2 tunnel.
- No Cloudflare Access app — par-rt-db carries its own auth (documented
  exception in `~/.claude/guides/infrastructure.md`).

## Deploy / update

The preferred path is `make deploy` from the repo root: it runs `make checkall`
first (the full gate), then rsyncs to lenny2 and runs `docker compose up -d
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

Source is synced to `/docker/par-rt-db` on lenny2 and built there (the host is
x86_64; do not `docker save` an arm64 image from a Mac).

```sh
# from the repo root on the workstation:
# .env/.env.* MUST be excluded — they are gitignored (don't exist in the
# local checkout) but hold the live secrets on lenny2, so `--delete` without
# these excludes would wipe them out.
rsync -az --delete \
  --exclude target/ --exclude .git/ --exclude .superpowers/ --exclude node_modules/ \
  --exclude .env --exclude '.env.*' \
  ./ root@lenny2.par-com.net:/docker/par-rt-db/

# on lenny2 (the .env there holds the secrets, mode 600):
cd /docker/par-rt-db
docker compose up -d --build
docker compose ps
curl -fsS http://127.0.0.1:8300/healthz | jq .
# -> {"status":"ok","version":"<crate-version>","git_commit":"<sha>","build_timestamp":..,
#     "started_at":..,"uptime_seconds":..,"postgres":true}
# A 503 with "status":"degraded"/"postgres":false means the server is up but
# Postgres is not — `curl -f` exits non-zero so this surfaces in scripts.
```

Then verify the public path: `curl -fsS https://rtdb.pardev.net/healthz | jq .`.

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

`RTDB_SUBS_VERIFY_SKIP_EVERY=N` (default 0 = off) shadow-verifies 1 skip in
every N: the query runs anyway and its result is compared against the last
pushed one. **Setting it in `.env` is not enough on its own** — compose's
`environment:` block is an explicit allowlist, so a new `RTDB_*` key must also
be forwarded there (this one is). After changing it, recreate the
container (`docker compose up -d server`) and confirm
`rtdb_subs_skip_verifications_total` starts climbing; if it stays 0 while skips
accumulate, the variable isn't reaching the process. A divergence logs at ERROR, increments the counter, and pushes the
corrected result (so it repairs, not just reports). Each verification costs the
Postgres round-trip the skip avoided. **Prod runs a permanent standing canary at
`RTDB_SUBS_VERIFY_SKIP_EVERY=200` (set 2026-07-30)** — a skipped update is
silent, so the verifier stays on as a detector rather than being toggled off.
After changing invalidation logic, temporarily lower it to N=20 for a few days
and confirm `rtdb_subs_missed_pushes_total` stays 0, then return it to 200.

## Monitoring

`GET /metrics` is the Prometheus scrape endpoint — plain text exposition,
aggregate-only (no per-db, no principal data), same auth posture as `/healthz`
(none). Content-negotiated on `Accept`: a browser (`text/html`) is served the
SPA's `index.html` when `RTDB_STATIC_DIR` is set; everything else (Prometheus
sends `application/openmetrics-text`, curl, API-only deploys) gets the
Prometheus text. Point a scraper at `https://rtdb.pardev.net/metrics`.

The subscription-invalidation canary above is the one alert to wire up: alert
on any increase of `rtdb_subs_missed_pushes_total` (only populated when
verification is on — prod runs `RTDB_SUBS_VERIFY_SKIP_EVERY=200`). The admin
JSON snapshot with per-db breakdowns (storage, subs, quota rejections) stays at
`GET /admin/metrics`, behind the admin key — do not scrape that from Prometheus,
since per-db labels would blow up cardinality.

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

- `POSTGRES_PASSWORD`, `RTDB_ADMIN_KEY` — `openssl rand -hex 32`. `RTDB_ADMIN_KEY`
  is also stored in parvault for admin CLI use.
- `RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET` — from parvault
  (`RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET`).
- `RTDB_GOOGLE_CLIENT_ID` / `RTDB_GOOGLE_CLIENT_SECRET` — from parvault
  (`RTDB_GOOGLE_CLIENT_ID` / `RTDB_GOOGLE_CLIENT_SECRET`); optional, leave blank
  to disable Google login. **Both must be passed to the server in
  `docker-compose.yml`'s `environment:` block** (they are, alongside the GitHub
  pair) — the server reads them at boot, so a change needs `docker compose up -d`
  to take effect.
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
curl -s -X POST https://rtdb.pardev.net/admin/create-db \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -d '{"name":"kanban"}'
curl -s -X POST https://rtdb.pardev.net/admin/push-schema \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -d '{"db":"kanban","schema":{...}}'
curl -s -X POST https://rtdb.pardev.net/admin/mint-token \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" -d '{"db":"kanban","name":"cli"}'
curl -s -X POST https://rtdb.pardev.net/admin/allowlist \
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
2. Point `RTDB_DATABASE_URL` at `rtdb_restored_<stamp>` in `.env`.
3. Restart the server (`docker compose up -d`).
4. Once stable, drop the old database at your discretion.

Credentials for `createdb`/`pg_restore` travel via the `PG*` env vars, never on
the argv.

## Troubleshooting

Common operator symptoms on the live deploy:

- **`/healthz` returns 503 with `"status":"degraded"` / `"postgres":false`** —
  the server process is up but cannot reach Postgres. Check
  `docker compose ps`, the `rtdb-pg` container health, and the
  `RTDB_DATABASE_URL` in `.env`. `curl -f` exits non-zero on the 503, so this
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

`docker compose down` stops the stack (the named volume `rtdb-pg` persists
data). To wipe data too: `docker compose down -v`.
