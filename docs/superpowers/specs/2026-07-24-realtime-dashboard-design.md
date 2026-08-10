# Realtime Dashboard (Data Browser + Metrics + Config) — Design

- **Status:** Implemented (2026-08-10) — backend phases 1–6 + frontend SPA live (served same-origin from `RTDB_STATIC_DIR`). Delivers FEATURE_MATRIX #18 ("Data browser dashboard") and expands it from a table browser into a full realtime ops dashboard.
- **Date:** 2026-07-24
- **Related:** FEATURE_MATRIX #18 (and the "Admin control plane" row); main design spec `2026-07-21-par-rt-db-design.md` ("Auth", "Wire protocol", "Deployment"); per-row authorization `2026-07-24-per-row-authorization-design.md`; implementation plans — the seven-phase series under `docs/superpowers/plans/2026-07-24-realtime-dashboard-phase{1-auth,2-metadata,3a-metrics,3b-opfeed,4-config,5-admin-docs,6-static}.md`.
- **Scope:** This document specifies the **backend surfaces and wire contract** the dashboard consumes. The frontend SPA itself is designed separately via the `/impeccable` skill in a follow-on phase; this spec does not prescribe its visual design, only the data it can read and the actions it can take. The dashboard's static assets are served by the server (§6).

## Summary

Build a self-hosted, same-origin web dashboard served by `rtdb-server` that turns the
existing admin control plane + document API into a realtime ops surface:

- **Data browser** — read/write documents across every database (insert / patch / replace /
  single-doc delete), row-bounded, with server-enforced guardrails.
- **Metrics** — live operational gauges (WS connections, subscriptions, DB pool, uptime),
  per-database inventory and storage size, throughput counters, and a realtime
  operation/activity feed.
- **Config** — read-only display of the running configuration plus a small set of
  safe, hot-reloadable knobs (no restart).
- **Access** — authenticated by a real human identity (GitHub/Google OAuth session on a
  server-wide **admin allowlist**), with the existing raw admin key retained for
  CLI/automation.

Everything the dashboard does that touches documents reuses the existing single-writer
committer, subscription fan-out, and per-row authorization plumbing — the dashboard is a
new *caller* of the existing core, not a second writer.

## Background & motivation

The server today is a pure JSON-API process: it serves no static assets, and the only way
to inspect or fix data is `psql`. FEATURE_MATRIX #18 flags the dashboard as the largest
remaining parity gap vs. Convex and a real DX advantage.

The existing building blocks the dashboard needs already exist but are incomplete as a
dashboard substrate:

- **Admin control plane** (`server/src/admin/` module): create-db, push-schema, list-dbs,
  mint/revoke-token, allowlist, export/import-db — all gated on a single global
  `RTDB_ADMIN_KEY` (constant-time compare). Gaps: **no schema read-back, no token
  listing, no stats, no metrics, no config read or mutation.**
- **Document plane** (`server/src/http_api.rs`, `server/src/ws.rs`): `/api/query`,
  `/api/mutate`, `/sync` (WS realtime) — authorized per-request via
  `resolve_bearer` + `authorize` against per-db machine tokens or OAuth user sessions,
  with per-row `ownerField` authorization (#20) applied through `owner_of`.
- **Committer** (`server/src/committer.rs`): the single per-db writer task. Every
  committed transaction funnels through `handle_mutate` or `handle_scheduled`, each of
  which calls `execute_txn` then `subs.fan_out(&outcome.write_set)`. This serialized
  choke point is the natural — and only correct — place to observe operations.
- **Config** (`server/src/config.rs`): entirely boot-time, env-driven, immutable.
- **Hosting**: no static serving; CORS is an `allowed_origins` allowlist baked into the
  router's `CorsLayer` at startup (`server/src/lib.rs`).

Two architectural facts shape this design. First, **the admin key is not a document
principal** — `resolve_bearer`/`authorize` know nothing about it, so the admin key alone
cannot read documents or subscribe over `/sync`. Second, **the committer is the
correctness core** (single writer, serialized); any new observation of writes must tap it,
not the handlers, or it will miss scheduled jobs and any future internal callers.

## Non-goals

- **The frontend build.** SPA design and implementation are a separate `/impeccable`-driven
  phase. This spec defines only the backend contract.
- **Durable audit log.** The op feed (§3) is in-memory and non-durable by design. The
  chosen tap location is the seam a future durable audit table could attach to without
  rework.
- **Bulk operations UI.** Multi-row delete/patch/upsert-many from the dashboard are out
  (Q4); the server additionally caps affected-docs per mutation (§5).
- **Runtime editing of boot-only config.** `port`, `database_url`, `admin_key`, and OAuth
  client secrets remain env-only and immutable at runtime (Q3).
- **Multi-tier dashboard roles.** A single admin tier (server-wide allowlist); no per-db
  dashboard permissioning.
- **Changing the existing allowlist/token model.** The admin allowlist is an additional
  server-wide layer; per-db machine tokens and per-db email allowlists behave exactly as
  today.

## 1. Auth & dashboard access model

### Admin principal

Introduce an `AdminPrincipal` resolved by an upgraded admin gate:

```rust
pub enum AdminPrincipal {
    /// The raw RTDB_ADMIN_KEY (constant-time compare). CLI/automation path.
    Key,
    /// An OAuth User principal present in rtdb_auth.admins. Browser path.
    User(auth::Principal),
}
```

`require_admin(state, headers) -> Result<AdminPrincipal, RtDbError>`:

1. Constant-time compare the bearer against `state.config.admin_key` → `AdminPrincipal::Key`.
2. Else `resolve_bearer(headers)`; if it yields a `Principal::User` whose **email** *or*
   **GitHub id** is in `rtdb_auth.admins` → `AdminPrincipal::User(principal)`.
3. Anything else (machine token, non-allowlisted/expired user, missing token) →
   `Unauthorized`.

Machine tokens are never admin. Every existing and new admin route switches from the
current `require_admin(headers, &key)` to the new `require_admin(state, headers)`.

### Admin allowlist

New table in the `rtdb_auth` schema, server-wide (distinct from the per-db allowlist):

```sql
CREATE TABLE rtdb_auth.admins (
    email     text PRIMARY KEY,   -- stored lowercase, matching auth::authorize
    github_id bigint,             -- optional; pairs with email for GitHub users
    added_at  bigint NOT NULL
);
```

Lookup matches on `email` (lowercased) **or** `github_id`, so an admin who authenticated
via Google (no GitHub id) is still recognized by email.

**Bootstrap (no chicken-and-egg):** on startup, idempotently insert every address in
`RTDB_ADMIN_EMAILS` (comma-separated env, `github_id = NULL`). After that, a one-shot
admin-key `POST /admin/admins {email}` adds yourself, then OAuth is usable thereafter.
`DELETE /admin/admins {email}` and `GET /admin/admins` round out CRUD — all admin-gated.

### CORS

External apps still use `RTDB_ALLOWED_ORIGINS` exactly as today, but the layer is rebuilt
to consult live config so `allowed_origins` is hot-reloadable (§4): `CorsLayer::new()
.allow_origin(AllowOrigin::predicate(...))`, where the predicate reads
`state.hot.load().allowed_origins` on every request. The layer is still constructed once
at router build time; only the origin decision is dynamic. WS remains CORS-exempt (Origin
is enforced at OAuth start, unchanged). The same-origin dashboard (§6) never needs an
origins entry of its own.

## 2. New admin-scoped API surface

All routes behind `require_admin`. Existing control plane (create-db, push-schema,
list-dbs, mint/revoke-token, allowlist, export/import) is unchanged.

| Method + path | Body / params | Returns | Fills gap |
|---|---|---|---|
| `GET /admin/dbs/{db}/schema` | — | current `SchemaDef` | no schema read-back today |
| `GET /admin/tokens?db=` | — | `[{id,name,createdAt,revoked}]` — **never** the secret | only mint/revoke exist |
| `GET /admin/dbs/{db}/stats` | — | table list + per-table row count + per-table & per-db storage size | no stats endpoint |
| `GET /admin/admins` | — | admin-allowlist members | new (CRUD) |
| `POST /admin/admins` | `{email, githubId?}` | `{ok}` | new |
| `DELETE /admin/admins` | `{email}` | `{ok}` | new |
| `POST /admin/db/{db}/query` | `{query}` | `QueryResult` (owner=`None`) | admin document read |
| `POST /admin/db/{db}/mutate` | `{txn, idempotencyKey?}` | `{results}` (owner=`None`) | data-browser write path |
| `GET /admin/metrics` | — | gauges + throughput + per-db summary snapshot | new |
| `WS /admin/stream` | `?db=&table=` query filters | multiplexed push (§3) | new |
| `GET /admin/ops/recent?db=&table=&n=` | — | recent op-feed ring (last N) | new |
| `GET /admin/config` | — | redacted config (§4) | new |
| `PATCH /admin/config` | hot-knob patch | new redacted config (§4) | new |

### Live document tables over `/sync`

Document realtime reuses the existing WebSocket, with two localized changes in
`server/src/ws.rs` only (the security core `auth::authorize` is untouched):

- The connection's principal is resolved once at the WS handshake (the bearer is fixed
  for the connection). Because the handler **re-runs `authorize` on every Subscribe and
  Mutate**, the admin short-circuit applies on each such call: an `AdminPrincipal::User`
  **bypasses the per-db `authorize` allowlist check** — an admin is authorized for every
  database, on every operation.
- Admin subscribers use **owner = `None`** (see all rows) on every subscribe and re-run,
  where a normal user uses `owner_of(principal)`. This keeps per-row `ownerField` filtering
  intact for every non-admin subscriber.

Non-admin principals on `/sync` behave exactly as today.

## 3. Metrics & op feed

### Metrics (`Metrics` in `AppState`)

Cheap lock-free atomics, snapshotted on demand:

- **Gauges:** `ws_connections` (inc/dec in the WS handler), `subscriptions`
  (`SubscriptionManager::count()` — a new `pub async fn count(&self) -> usize` summing
  subscribers across databases), sqlx pool `size()`/`idle()`, plus the existing
  `/healthz` values (uptime, version, commit, postgres reachable).
- **Counters:** `queries_total`, `mutations_total`, `schedules_fired_total`,
  `uploads_total` (incremented at the existing handler / committer call sites).
- **Throughput:** rolling 60×1s windows (rotated by a background task) yielding per-second
  rates for queries and mutations.

`GET /admin/metrics` returns one JSON snapshot (gauges + counters + throughput + a
per-db summary derived from §2 stats) for initial load and for any client that prefers
polling.

### Op feed (`OpFeed`, approach A)

A `tokio::sync::broadcast::Sender<OpEvent>` plus a lock-guarded ring buffer, held in
`AppState`:

```rust
pub struct OpEvent {
    pub db: String,
    pub table: String,
    pub op: OpKind,            // Insert | Patch | Replace | Delete
    pub doc_id: String,
    pub ts: i64,               // ms
    pub principal: PrincipalKind, // Admin | Machine | User | Scheduled
}
```

**Tap placement (load-bearing):** publish at the two grounded sites, immediately after a
successful `execute_txn`, fed by `outcome.write_set` (which already enumerates the
affected docs):

- `committer::handle_mutate` (`server/src/committer.rs`, after line ~295)
- `committer::handle_scheduled` (`server/src/committer.rs`, after line ~348)

One `OpEvent` per affected doc. `broadcast::send` is non-blocking and a no-op when there
are no subscribers — **zero hot-path cost when the dashboard is closed.** Because both
scheduled jobs and interactive mutations pass through these two sites, the feed is
complete (this is exactly why the tap is at the committer and not the handlers).

**Ring buffer:** last `RTDB_OP_FEED_RING` ops (default 500), kept in memory for late
connect / reconnect replay. **Non-durable** — a restart clears it.

### `/admin/stream` protocol (admin-gated WS)

On connect: replay the ring filtered by `?db=`/`?table=` query params, then attach a
fresh `broadcast` receiver for live events. A spawned task pushes a `{kind:"gauges"}`
snapshot roughly every 1 s. Message envelope:

```jsonc
{ "kind": "op",         "event": { /* OpEvent */ } }
{ "kind": "gauges",     "gauges": { /* Metrics snapshot minus throughput */ } }
{ "kind": "throughput", "buckets": [ /* 60 per-second rates */ ] }
```

`GET /admin/ops/recent?db=&table=&n=` returns the same filtered ring as JSON for an
initial fill before (or instead of) opening the socket.

## 4. Config: read-only display + safe hot-reload

Split `Config` into two layers:

- **`BootConfig`** (env, immutable for the process life): `port`, `database_url`,
  `admin_key`, `public_url`, OAuth client id/secret, GitHub base/api URLs,
  `RTDB_STATIC_DIR` (§6). Loaded once in `Config::from_env`.
- **`HotConfig`** (runtime-mutable), held in `AppState` as `Arc<ArcSwap<HotConfig>>`:
  `allowed_origins: Vec<String>`, `session_ttl_days: i64`, `max_file_size: usize`.

Persisted in a new single-row table:

```sql
CREATE TABLE rtdb_config (id int PRIMARY KEY DEFAULT 1 CHECK (id = 1), hot jsonb NOT NULL);
```

On startup, load the row into `ArcSwap` (or fall back to env-seeded defaults if absent).
Every handler that needs a hot value reads `state.hot.load()` (deref to the current
`Arc<HotConfig>`), so a swap takes effect on the very next request with no restart:

- `GET /admin/config` — full redacted config (boot values masked: `admin_key` masked,
  OAuth secrets → configured-bool; hot values shown in full; plus version/commit and
  admin-allowlist member list).
- `PATCH /admin/config` — accepts a subset patch (`allowedOrigins`, `sessionTtlDays`,
  `maxFileSize`); validates, writes the merged JSON to `rtdb_config`, swaps, returns the
  new redacted config. Unknown/immutable fields → `BadRequest`.

**Hot-reload consumers:** the CORS `AllowOrigin::predicate` (§1) reads
`hot.load().allowed_origins`; the WS/auth session minting reads
`hot.load().session_ttl_days`; the upload handler reads `hot.load().max_file_size`. The
per-db and admin allowlists are not in `HotConfig` — they stay live `rtdb_auth` queries
(as the per-db allowlist already is).

## 5. Data-browser write path & guardrails

Writes go through `POST /admin/db/{db}/mutate` → `state.committers.mutate(db,
idempotencyKey, txn, owner=None)`. This is the **existing** committer path: single writer,
subscription fan-out, idempotency dedup, and the op-feed tap (§3) all fire unchanged.
`owner=None` means per-row `ownerField` is bypassed for admin writes (admin sees/touches
all rows), matching the admin read path.

- `patch` / `replace` / `delete` target a single document id by the DSL's existing
  semantics; `upsert` is the only op that can match multiple documents.
- **Server-side guardrail (the real backstop, not trusted to the UI):** a max-affected-docs
  cap per mutation (`RTDB_MAX_AFFECTED_DOCS`, default 100). A mutation whose committed
  `write_set` exceeds the cap is rejected and rolled back within the same serialized
  committer turn → `BadRequest`. (Exact pre-check vs. post-check-and-rollback mechanism is
  a plan detail; the invariant is: an over-cap mutation never becomes durable.)
- **UI-side guardrails (impeccable phase):** typed confirmation for delete/replace, a
  "writes enabled" toggle, take-N-only query construction. The server does not rely on
  these.

## 6. Static hosting & transport

- `tower-http::services::ServeDir::new(RTDB_STATIC_DIR)` with an SPA fallback to
  `index.html`, mounted **last** in `build_router` (after API/admin/ws/auth) so it can
  never shadow a real route. Unknown `GET` paths fall through to `index.html` for
  client-side routing; registered routes keep returning their own JSON/404s.
- **`RTDB_STATIC_DIR`** (default `./static`). Unset or empty directory ⇒ the server runs
  API-only (no change to today). **Hot-swap:** drop new build artifacts into the folder
  and refresh the browser — no recompile, no restart. In the `docker compose` deploy the
  folder is a mounted volume, so updating the frontend needs no image rebuild.
- **Cache headers:** `index.html` no-cache; hashed asset bundles long-cache.
- **Same-origin** ⇒ dashboard→API/WS calls need no `allowed_origins` entry.

## Invariants preserved / added

- **Single writer intact.** The dashboard's document writes route through the existing
  committer (`committers.mutate`); the op-feed tap only *observes* after `execute_txn` and
  never writes document tables. No second writer is introduced.
- **Per-row auth preserved.** Non-admin principals keep `owner_of` filtering on read,
  mutate, and subscription re-run. Admin uses `owner=None` deliberately and only through
  admin-gated routes.
- **SQL safety unchanged.** All new lookups bind values via `$n` and double-quote
  identifiers; the new stats queries use `pg_total_relation_size(format('%I.%I', …))` for
  safe, injection-free sizing. `fetch_optional` for any lookup that can miss.
- **Error envelope unchanged.** Every new failure is an `RtDbError` `{code, message}`;
  client-facing 500s stay generic (no sqlx/serde leakage). Redaction in
  `GET /admin/config` is enforced structurally (the redacted type omits secret fields),
  not by remembering to mask at each call site.
- **New invariant — op-feed completeness.** Every durable document mutation appears in the
  feed exactly once, because the tap is at the sole two commit sites. Any future code path
  that commits a document txn must publish through one of these sites (or be added as a
  third).

## Phasing (backend first; frontend via `/impeccable` after)

Each phase is independently verifiable and committed atomically; `make checkall` gates
each. Sub-agent isolation may be used for independent batches.

1. **Auth foundation** — `AdminPrincipal`, `require_admin` upgrade, `rtdb_auth.admins` +
   `RTDB_ADMIN_EMAILS` bootstrap, `GET/POST/DELETE /admin/admins`. Verify: key path still
   works, OAuth-admin admitted, non-admin rejected, expired session rejected.
2. **Metadata read-back** — `GET /admin/dbs/{db}/schema`, `GET /admin/tokens`,
   `GET /admin/dbs/{db}/stats`. Verify: schema round-trips, token list never leaks the
   secret, stats sizes match `pg_total_relation_size`.
3. **Metrics + op feed** — `Metrics` struct + counters, `SubscriptionManager::count()`,
   `OpFeed` + ring, the two committer tap sites, `WS /admin/stream`, `GET /admin/metrics`,
   `GET /admin/ops/recent`. Verify: an admin mutate emits exactly the expected op event
   on the stream; ring replays on reconnect.
4. **Config** — `BootConfig`/`HotConfig` split, `rtdb_config` table, `ArcSwap` wiring,
   `GET /admin/config`, `PATCH /admin/config`, dynamic `AllowOrigin::predicate`. Verify:
   PATCH changes a hot value and the next request observes it; secrets stay redacted.
5. **Admin document access** — `POST /admin/db/{db}/query|mutate`, `/sync` admin bypass +
   owner=`None`, `RTDB_MAX_AFFECTED_DOCS` cap. Verify: admin reads all rows on an
   `ownerField` table; over-cap mutation rejected and not durable.
6. **Static hosting** — `ServeDir` + SPA fallback + `RTDB_STATIC_DIR` + deploy volume
   wiring + cache headers. Verify: a dropped `index.html` is served same-origin; an empty
   dir leaves the API unaffected.
7. **(separate, `/impeccable`-driven)** Frontend design + build, consuming the contract
   above.

## Testing strategy

Mirrors the existing integration-test layout (`server/tests/`). Tests share one Postgres
and isolate via uniquely-named databases; never drop a database/schema they did not create.

- New `dashboard_test.rs` (or per-concern binaries) covering: admin auth (key, OAuth-admin
  via the existing `mint_user_session` helper, non-admin rejected), schema read-back,
  token list (assert the secret is absent), stats, metrics snapshot, op-feed ring replay,
  config read + hot-reload (assert next request sees new value; secrets redacted), admin
  query/mutate (owner bypass on an `ownerField` table; op-feed fires), `/admin/stream` WS
  round-trip (mutate → receive `{kind:"op"}`), and the affected-docs cap.
- The op-feed completeness test doubles as the guard against future code paths that try to
  commit document txns outside the two tap sites.

## Documentation

- **FEATURE_MATRIX #18** ❌→✅, marking the backend shipped and the frontend 🚧 until the
  impeccable phase lands; update the "Admin control plane" row to note the dashboard and
  the new admin endpoints.
- **`CLAUDE.md`** — add the dashboard surface, the admin-allowlist auth path, the op-feed
  tap as a new invariant, and static serving (`RTDB_STATIC_DIR`).
- **Server README + deployment runbook** — dashboard URL, `RTDB_STATIC_DIR` volume mount,
  `RTDB_ADMIN_EMAILS` bootstrap, hot-reload knobs, and the new admin endpoints.
- This spec + the implementation plan committed under `docs/superpowers/specs/` and
  `docs/superpowers/plans/`.

## Success criteria

- An operator can log in via GitHub/Google (if allowlisted), browse every database and
  table, read/write individual documents with confirmations, and watch writes appear live
  in the op feed and in open table views — all in the browser, with `psql` no longer
  needed for routine inspection or fixes.
- Live gauges, throughput, and per-db sizes reflect reality; hot-reloadable config changes
  take effect without a restart; the frontend updates from a dropped static folder without
  a restart or image rebuild.
- `make checkall` passes; no new writer outside the committer; no secret ever reaches a
  dashboard response.
