# par-rt-db dashboard

The operator console SPA for par-rt-db — a dark, realtime **Instrument Manual**
console: data browser, schema viewer, live metrics, the operation feed, and hot
config. Served same-origin by the server from `RTDB_STATIC_DIR` in production.

Visual world: [`../DESIGN.md`](../DESIGN.md). Product: [`../PRODUCT.md`](../PRODUCT.md).

## Stack

Vite + React + TypeScript, managed with **bun**. Hand-authored, token-driven CSS
(no Tailwind / shadcn — the world mandates bespoke components).

## Develop

The SPA talks to a par-rt-db backend. In dev, Vite proxies `/api`, `/admin`,
`/sync`, `/auth`, `/storage`, `/healthz` to the backend so the OAuth session
cookie and the `/sync` + `/admin/stream` WebSockets behave same-origin.

```bash
make install          # bun install + build the linked @par-rt-db/client SDK
make dev              # vite on http://127.0.0.1:8310
```

Point at a different backend (default `http://127.0.0.1:8300`):

```bash
RTDB_BACKEND=http://127.0.0.1:8300 make dev
# or against the live instance (CORS/cookie caveats apply):
RTDB_BACKEND=https://rtdb.pardev.net make dev
```

Run a local backend to develop against:

```bash
make -C .. dev-db-up   # dev Postgres on 127.0.0.1:55434
cd ../server && cargo run
```

## Build

```bash
make build            # builds the linked SDK, then vite build → dist/
```

`dist/` is what gets mounted at `RTDB_STATIC_DIR` on the server (hashed assets
→ immutable cache; `index.html` → no-cache). See the server `CLAUDE.md` static
hosting note.

## Verify

```bash
make typecheck && make lint && make fmt-check && make test
```

The repo-wide gate is `make -C .. checkall`.
