# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

The primary (and, for the foreseeable, sole) user is the operator who runs a par-rt-db instance — Paul, a developer/self-hoster managing personal infrastructure. They wear every hat: deploy the server, define and evolve database schemas, mint machine tokens, debug document data, watch live behavior, and tune operational config. They are technical, fluent in JSON/query semantics, and treat the dashboard as a fast, always-available control surface for a service they own end-to-end. Non-admin end-users have no dashboard (confirmed: admin/operator console only).

## Product Purpose

par-rt-db is a self-hosted, Convex-inspired realtime document database. One generic Rust server hosts many named databases; clients push a schema and send a declarative JSON DSL — typed queries, subscriptions, atomic multi-step transactions — over WebSocket (`/sync`) or one-shot HTTP, and receive live query updates as data changes. There is no embedded JS runtime and no per-app server code; one server serves every app.

The dashboard is the single operator console for that server: authenticate, manage databases and their schemas, browse and mutate documents, observe realtime metrics and the live operation feed, and edit runtime configuration — without touching the CLI, SQL, or config files.

Success means the operator can do every day-to-day management and debugging task from the browser, fast, against the live instance, with confidence about what changed.

## Positioning

A self-hosted realtime document DB the operator fully controls — Convex's developer model (declarative queries, realtime subscriptions, schema-as-source-of-truth) without a managed cloud, a vendor account, or per-app backend code. The dashboard is the proof that one generic, self-hosted server is a complete product, not just an engine.

## Operating Context

- Reference deployment shape: `docker compose` behind a Cloudflare tunnel (host-agnostic; see `deploy/README.md`); the dashboard is a same-origin SPA served by the server itself from `RTDB_STATIC_DIR`. In the docker deploy the SPA is **baked into the image** (the `dashboard` build stage in `Dockerfile` copies `dist/` to `/app/dashboard-dist`), so a frontend change ships via `docker compose up -d --build` (image rebuild + server container recreate), not a live-mounted volume.
- The operator authenticates either with the admin key (machine) or as an OAuth'd admin (GitHub, Google, GitLab, Microsoft, Apple, or a configured OIDC provider — allowlisted as admin).
- Every database is isolated and named; the operator juggles several (e.g. the projects board, app datastores). Documents are JSON; indexed fields become typed Postgres columns, the rest lives in a `doc` jsonb column merged in at read time.
- Realtime is central: subscriptions re-run on every write, and the op feed surfaces durable mutations as they happen. The dashboard must feel live, not request/response.
- Operational reality the console reflects and respects: one Postgres, one writer (the committer), hot config that applies without restart, optional per-row ownership auth on some tables.

## Capabilities and Constraints

Confirmed backend surfaces the dashboard consumes (all shipped, HTTP/WS):

- **Auth & session** — admin-key or OAuth admin login; `/auth/*` flows; session TTL; logout.
- **Databases** — list/create databases; per-db metadata (schema read-back, machine tokens, table + row counts).
- **Data browser** — read/query and mutate documents per database/table via admin doc routes (`POST /admin/db/{db}/query|mutate`, `owner=None`); mutations bounded by `RTDB_MAX_AFFECTED_DOCS` (server boot config, default 100), which counts the worst-case affected documents, not raw step count — per-id steps (`insert`/`patch`/`replace`/`delete`/`expectVersion`/`expectAbsent`/`upsert`) count one doc each, `schedule`/`cancelSchedule` steps count zero (they touch no documents), while each `patchByQuery`/`deleteByQuery` step counts up to its `limit` (ceiling `MAX_BY_QUERY_ROWS = 1000`).
- **Schema viewer** — the compiled schema for each database/table (typed indexed fields, `ownerField`, table stats).
- **Metrics** — `GET /admin/metrics`, instance-wide counters and gauges.
- **Live op feed** — `GET /admin/ops/recent` (recent) and `WS /admin/stream` (streaming) durable document mutations as they happen.
- **Hot config** — `GET/PATCH /admin/config` for runtime-mutable `allowed_origins`, `session_ttl_days`, `max_file_size`, `idempotency_ttl_ms`, and the three per-database quota caps `max_tables_per_db` / `max_storage_bytes_per_db` / `max_subs_per_db` (`0` = unlimited; ENH-011) — secrets structurally redacted; admin allowlist management.
- **Realtime over WS** — the SPA may subscribe like any client (`/sync`) to watch data move.

Constraints:

- Solo operator; no multi-admin collaboration or per-actor audit UI is required (confirmed).
- Same-origin: the SPA's API/WS calls need no CORS entry; it talks to the server that serves it.
- No fabrication of stats, customers, or benchmarks — the dashboard shows only real, server-reported data.

## Brand Commitments

- Product name **par-rt-db**, a self-hostable realtime document database.
- No existing visual identity, logo, or design system for par-rt-db has been declared binding — the visual world is established in this work.

## Evidence on Hand

- Protocol, DSL, and semantics: `README.md`, `FEATURE_MATRIX.md`, and `wire-corpus/` (the original 2026-07-21 design spec is a historical record, superseded by those).
- Dashboard backend design: `docs/superpowers/specs/2026-07-24-realtime-dashboard-design.md` (the six-phase surface contract).
- Server source: `server/src/` (`auth/`, `admin/`, `http_api.rs`, `ws.rs`, `committer/`, `schema.rs`, `query/`, `txn.rs`, `config.rs`, `storage.rs`).
- Four client implementations of the wire contract: `server/src/protocol.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, and `python-client/src/par_rt_db/wire.py` (the SPA will speak this protocol via the ts-client SDK).
- `FEATURE_MATRIX.md` (#18) — the parity/feature contract.
- Deployment shape: self-hosted `docker compose` behind a Cloudflare tunnel (`deploy/README.md`); no specific hosted instance is assumed.
- Absences to respect: no real usage metrics, customers, or testimonials exist; the dashboard must not synthesize social proof.

## Product Principles

1. **Operator-first, always.** Every screen serves the person running the instance — speed, precision, and confidence over marketing or onboarding flourish.
2. **Live by default.** Data, metrics, and the op feed reflect the running server in real time; staleness is a defect.
3. **Reflect the model truthfully.** Databases, tables, schemas, ownership, and the single-writer reality are shown as they actually are, never abstracted into something misleading.
4. **One coherent console.** Six backend surfaces are one product with consistent navigation, terminology, and interaction — not six glued-together tools.
5. **Safe by construction.** Mutations are explicit and capped; destructive actions confirm; config edits validate before they apply.

## Accessibility & Inclusion

Standard web accessibility expected for a technical operator tool (keyboard navigation, sufficient contrast, readable type, focus states). No specialized standard mandated beyond that; refine during design.
