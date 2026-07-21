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
rsync -az --delete \
  --exclude target/ --exclude .git/ --exclude .superpowers/ --exclude node_modules/ \
  ./ root@lenny2.par-com.net:/docker/par-rt-db/

# on lenny2 (the .env there holds the secrets, mode 600):
cd /docker/par-rt-db
docker compose up -d --build
docker compose ps
curl -fsS http://127.0.0.1:8300/healthz     # -> ok
```

Then verify the public path: `curl -fsS https://rtdb.pardev.net/healthz`.

## Secrets (`/docker/par-rt-db/.env`, not committed)

- `POSTGRES_PASSWORD`, `RTDB_ADMIN_KEY` — `openssl rand -hex 32`. `RTDB_ADMIN_KEY`
  is also stored in parvault for admin CLI use.
- `RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET` — from parvault
  (`RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET`).
- `RTDB_ALLOWED_ORIGINS` — the SPA origin(s); adjust when the client's final
  origin is known, then `docker compose up -d` to apply.

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
