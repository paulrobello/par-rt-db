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

## Secrets (`/docker/par-rt-db/.env`, not committed)

- `POSTGRES_PASSWORD`, `RTDB_ADMIN_KEY` — `openssl rand -hex 32`. `RTDB_ADMIN_KEY`
  is also stored in parvault for admin CLI use.
- `RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET` — from parvault
  (`RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET`).
- `RTDB_ALLOWED_ORIGINS` — the SPA origin(s); adjust when the client's final
  origin is known, then `docker compose up -d` to apply.
- `RTDB_BUILD_COMMIT` (optional) — git short sha baked into `/healthz`. Set it
  to the deployed commit before `docker compose up -d --build`, e.g.
  `RTDB_BUILD_COMMIT=$(git rev-parse --short HEAD)` (run on the workstation
  that has `.git`, before rsync). If unset, `/healthz` reports
  `git_commit: "unknown"`.

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
