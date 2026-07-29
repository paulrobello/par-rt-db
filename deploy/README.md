# Deploying par-rt-db to lenny2

lenny2 is a standalone Docker host (plain `docker compose`, not Swarm). Public
traffic reaches it through the host `cloudflared` tunnel, which routes
`rtdb.pardev.net` -> `http://localhost:8300`. No ports 80/443 are opened on the
VPS and no reverse proxy is needed — TLS is terminated at Cloudflare's edge.

## One-time DNS/tunnel wiring (already done 2026-07-21)

- Proxied CNAME `rtdb.pardev.net` -> `<lenny2-tunnel-id>.cfargotunnel.com`
  (overrides the `*.pardev.net` wildcard that points at lenny1).
- Tunnel ingress rule `rtdb.pardev.net` -> `http://localhost:8300` appended to
  the lenny2 tunnel.
- No Cloudflare Access app — par-rt-db carries its own auth (documented
  exception in `~/.claude/guides/infrastructure.md`).

## Deploy / update

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
# -> {"status":"ok","version":"0.1.0","git_commit":"<sha>","build_timestamp":..,
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
be forwarded there (this one is, as of 0609012). After changing it, recreate the
container (`docker compose up -d server`) and confirm
`rtdb_subs_skip_verifications_total` starts climbing; if it stays 0 while skips
accumulate, the variable isn't reaching the process. A divergence logs at ERROR, increments the counter, and pushes the
corrected result (so it repairs, not just reports). Each verification costs the
Postgres round-trip the skip avoided — after changing invalidation logic, set
N=20 for a few days and confirm the counter stays 0, then set it back to 0.

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

## Backups

Nightly `pg_dump` of the `rtdb` database from the `postgres` service to the host
backup path (add to cron); data also persists in the `rtdb-pg` named volume.

## Rollback

`docker compose down` stops the stack (the named volume `rtdb-pg` persists
data). To wipe data too: `docker compose down -v`.
