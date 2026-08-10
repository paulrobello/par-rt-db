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

## Operator guide

The console is the operator UI for a running par-rt-db instance — same-origin
when served by the server from `RTDB_STATIC_DIR`, or via the Vite dev proxy
against any backend. Everything below is operator-facing; for dev/build see the
sections above.

### Logging in

Two login methods, both funneled through `authenticate_admin` on the server:

- **Admin key** — paste a value of `RTDB_ADMIN_KEY` (the boot env var set on
  the server). Covers the control plane (databases, schema, metrics, op feed,
  config, admins) via the HTTP `/admin/*` API. The raw key is **not** accepted
  on `/sync`, so the **data browser falls back to ~2s polling** of
  `POST /admin/db/{db}/query` rather than subscribing.
- **OAuth (GitHub / Google / GitLab / Microsoft / Apple / OIDC)** — sign in
  with an account whose email is on the `RTDB_ADMIN_EMAILS` allowlist (or has
  been added via the Admins page since boot). Available providers are those
  whose env-var pairs are set on the server (each is independently optional).
  The session token issued by `/auth/*` **is** accepted on `/sync`, so the
  **data browser subscribes for true realtime**.

The two methods differ only in how the data browser reads documents; the op
feed, metrics, and database list stream over the same `/admin/stream`
WebSocket either way (the admin bearer rides the `Sec-WebSocket-Protocol`
subprotocol).

Regardless of method, the admin bearer lives only in React state — reload the
page and you re-authenticate (a deliberate trade-off against `localStorage`
theft; see `SEC-001`). Sign out clears it from memory and, for OAuth,
best-effort calls `POST /auth/logout`.

### The dashboard surfaces

Top-level nav (left rail) plus the per-database pages reached by drilling into
a database. Nineteen routes mirror the admin API surface:

| Surface | Path | What it does |
| --- | --- | --- |
| **Databases** | `/` | List, create, clone, and delete databases; drill into one for stats, schema, storage, schedules, webhooks, audit, and backups. |
| **Database overview** | `/dbs/:db` | Per-database stats (row counts, storage size, quota usage), recent ops, and quick links. |
| **Schema** | `/dbs/:db/schema` | The pushed schema for one database (tables, fields, indexes, `ownerField`/`collaboratorsField`/`authorize`, `ttl`). |
| **Schema history** | `/dbs/:db/schema/history` | Newest-first schema snapshots; diff against the current shape; restore-confirm to reconcile. |
| **Migrate** | `/dbs/:db/migrate` | Guided destructive-shape migrate (dry-run → review → apply); see [Schema migration](#schema-migration-guided) below. |
| **Data browser** | `/dbs/:db/tables/:table` | Live documents in one table. OAuth = realtime over `/sync`; admin-key = ~2s poll. Insert/patch/delete under the `RTDB_MAX_AFFECTED_DOCS` cap. |
| **Metrics** | `/metrics` | Server-wide gauges (queries, mutations, uploads, pool size/idle, uptime) + quota-rejection counters (`rtdb_quota_rejections_total{kind=tables\|storage\|subs}`). |
| **Op feed** | `/ops` | Newest-first document operations across all databases (`GET /admin/ops/recent`). Also streamed live into the right rail. |
| **Scheduled jobs** | `/scheduled` | Lists scheduled/cron jobs across databases; cancel/pause/resume controls. |
| **Storage** | `/storage` | Per-database blob browser; size, sha256, contentType, createdAt; delete/revoke. |
| **Subscriptions** | `/subscriptions` | Live-query inspector — every active subscription across databases with its read-set class and skip/re-run counters. |
| **Sessions** | `/sessions` | Active interactive sessions (OAuth/anonymous/admin-key) across the instance — filter by `user`, revoke a single session or every session for a user (`GET/DELETE /admin/sessions`). `token_hash` is a non-reversible sha256 digest. |
| **Tokens** | `/tokens` | Per-database machine tokens (mint with optional `expiresAt`/`readOnly`/`tables` scoping, revoke, list — no secrets returned). |
| **Webhooks** | `/webhooks` | Per-database webhook CRUD + recent delivery attempts (outbox drain). Requires `RTDB_WEBHOOKS_ENABLED=true`. |
| **Query console** | `/console` | Free-form admin query/mutate against any database (`POST /admin/db/{db}/query\|mutate`, `owner=None`). |
| **Config** | `/config` | Hot knobs (`allowed_origins`, `session_ttl_days`, `max_file_size`, `idempotency_ttl_ms`, `max_tables_per_db`, `max_storage_bytes_per_db`, `max_subs_per_db`), server build info, and provider configuration status. PATCH persists + swaps live (no restart). |
| **Admins** | `/admins` | Manage the OAuth admin allowlist (`RTDB_ADMIN_EMAILS` + any runtime additions). |
| **Audit** | `/audit` | Durable audit log viewer (`ts_ms, db, table, op, doc_id, principal, source`). Requires `RTDB_AUDIT_LOG_ENABLED=true`. |
| **Backups** | `/backups` | Manual `pg_dump` trigger + dump list, download, delete, and restore into a fresh `rtdb_restored_<stamp>` DB. Requires `RTDB_BACKUP_ENABLED=true`. |

### Schema migration (guided)

Destructive/type-changing schema transformations (rename, type coercion, removal,
default backfill) are a deliberate admin operation separate from the additive
schema push. The dashboard's migrate flow is **dry-run-first**: you compose a
directives list, preview the per-directive report (affected rows, cast failures,
sample before/after) and the derived resulting schema, then apply the same bytes
once you've reviewed them.

The flow lives beside the schema viewer on a database. Directives cover
`renameField`/`renameTable` (no data loss), `changeType` (closed cast matrix +
optional `default` for atomic-fail-vs-substitute), `dropField`/`dropTable`/
`dropIndex`, `setDefault` (one-time backfill), and `evalExpr` (a scoped raw-SQL
doc-rewrite escape — one table's `doc` jsonb, no joins/DDL verbs). Because the
server runs the migrate inside the committer's serialized turn, live queries
re-run and push, and the op feed / audit log / webhook outbox all fire on the
rewrite — the same guarantees as a regular mutation. A snapshot of the reviewed
directives is locked at preview time so Apply can't send unpreviewed bytes. See
the design spec at
[`../docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md`](../docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md).

### The mutation cap

Mutations submitted from the data browser are bounded by `RTDB_MAX_AFFECTED_DOCS`
(server boot config, default **100**), which counts the worst-case number of
documents a transaction could touch — not the raw step count. Per-id steps
(`insert`/`patch`/`replace`/`delete`/`expectVersion`/`expectAbsent`/`upsert`)
count one document each; each `patchByQuery`/`deleteByQuery` step counts up to
its `limit` (default and ceiling `MAX_BY_QUERY_ROWS = 1000`). An over-budget
transaction is rejected **before** the committer — an over-cap write never
becomes durable. Non-admin mutations are bounded by `MAX_STEPS = 1024` (step
count), `MAX_BY_QUERY_STEPS_PER_TXN = 16`, and a per-transaction aggregate
budget of `MAX_AFFECTED_ROWS_PER_TXN = 10000` rows (SEC-104); none of those are
affected by this knob. Raise `RTDB_MAX_AFFECTED_DOCS` only if a documented
workflow legitimately needs a larger single-transaction footprint.
