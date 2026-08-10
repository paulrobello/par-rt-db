# Operator Console Pages + Token Scoping + Subscription Inspector — Design

**Date:** 2026-08-04
**Status:** Implemented (2026-08-10)
**Covers:** ENH-005 (scoped & time-limited tokens, incl. table-level scoping), ENH-003 (webhook management page, full spec), ENH-004 (audit-log page, filterable), ENH-010 (live subscription inspector, incl. per-db counters), and a new Python admin client (full parity).

## 1. Overview

Four enhancements done as a batch, plus a new Python admin surface the batch forces into existence. The user selected the maximum scope on every open decision:

- **ENH-005**: expiry + read-only **and** table-level scoping.
- **ENH-003**: full spec — CRUD + edit + delivery-status (new `PUT` + deliveries `GET`).
- **ENH-004**: filterable by db/table/op/principal/source (server endpoint gains filter params).
- **ENH-010**: per-subscription metadata + global counters **and** new per-db counters.
- **Python**: build the admin client now (full parity with ts-client `RtDbAdminClient`).

All five packages are touched (server, ts-client, rust-client, python-client, dashboard). ENH-005 changes the auth core and the wire contract; ENH-010 extends the metrics surface; the rest are additive endpoints + client methods + dashboard pages.

## 2. ENH-005 — Scoped & time-limited machine tokens

### 2.1 Data model

Additive columns on `rtdb_auth.machine_tokens` (`server/src/db.rs:99`), with safe defaults so existing tokens are unaffected:

- `expires_at BIGINT NULL` — epoch ms; NULL = never expires.
- `read_only BOOLEAN NOT NULL DEFAULT false`.
- `tables TEXT[] NULL` — allowlist of table names; NULL or empty = all tables.

The `ensure` statement for this table is extended (idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...`). `mint_token` (`auth/tokens.rs:9`) takes the new optional fields and writes them.

### 2.2 Capability threading (the load-bearing change)

Today `Principal::Machine { db, token_id }` (`auth/mod.rs:19`) carries no capabilities and `authorize` (`auth/mod.rs:74`) is binary (db-match + not-revoked, checked live on every call).

`resolve_bearer` (`auth/mod.rs:42`) already does the token-hash lookup. It is extended to fetch the full token row in that one query — `id, db_name, read_only, tables, expires_at, revoked` — validate expiry and revocation, and construct `Principal::Machine { db, token_id, read_only, tables }`. Expiry + revocation also remain live checks in `authorize` (it re-queries every call, so a token that expires or is revoked mid-session is caught on the next op).

### 2.3 Enforcement points

These mirror the existing per-row-auth (`ownerField` / `authorize`-predicate) enforcement boundary:

- **Expiry / revocation / db-match**: unchanged location (`authorize`), with `expires_at` added to the live SQL predicate (`... AND (expires_at IS NULL OR expires_at > $now)`).
- **Read-only**: reject at the write gates. WS `Mutate` and `Schedule` per-op re-run (`ws.rs:358, 430, 494, 552, 585`), HTTP mutate handlers (`http_api.rs:78, 135, ...`), and storage upload/delete. A `Principal::read_only()` helper keeps the check clean. Admin principals bypass.
- **Table scoping**: a new `authorize_table(&principal, table) -> Result<(), RtDbError>` helper, called where the table is known — the read path (`query.rs`), each mutation step (`txn.rs`), and subscription registration (`subs.rs`) — adjacent to the existing per-row-auth checks. Denied → `Forbidden`/403. NULL/empty `tables` means all tables (no-op).

The capability checks apply to **machine-token principals**: a token bypasses per-row rules (no user identity) but is still bound by its own capabilities (read-only, tables) — a read-only token cannot mutate. **Scheduled jobs** are system-initiated (no principal, `owner=None`) and have no token, so token capabilities do not apply to them; they remain subject only to the db-level gate. The db-level allowlist/token/session gate still runs first in all cases.

### 2.4 Wire shapes (byte-identical mirror set)

`MintTokenRequest` gains optional `expiresAt?: number`, `readOnly?: boolean`, `tables?: string[]`. `TokenRow` gains the same three fields. The mirror set: server `admin.rs` (structs at `admin.rs:231-264, 952-964`), `ts-client/src/admin.ts:36-41,200`, `rust-client/src/wire.rs:473,543`, `dashboard/src/lib/types.ts:98-103`, and the new `python-client` admin module. Casing stays camelCase (`expiresAt`, `readOnly`, `tokenId`, `createdAt`) — the protocol's non-uniform casing is load-bearing.

Tokens appear on the WS wire only as opaque `token: Option<String>` on the `Auth` frame (`protocol.rs:14-24`) — unchanged; the new fields are HTTP-admin only.

### 2.5 Dashboard

No token UI exists today (only a vestigial `listTokens`/`TokenRow` nothing renders). Build a **Tokens page** (`dashboard/src/pages/TokensPage.tsx`): db selector, mint form (name + expiry datetime + read-only toggle + tables multi-select), list with revoked/expiry badges, revoke action. Add `mintToken`/`revokeToken` to the dashboard `AdminClient` (`dashboard/src/lib/admin.tsx`), mirroring `ts-client`. Register the route (`App.tsx:42-58`) and nav entry (`AppShell.tsx:9-19`). Follows the `StoragePage`/`ScheduledJobsPage` per-db pattern.

### 2.6 Tests

- Token with `read_only=true` rejected on mutate (WS + HTTP) and on storage write; allowed on query/subscribe.
- Token with `tables=["a","b"]` rejected querying/mutating table `c`; allowed on `a`/`b`.
- Expired token (`expires_at` in the past) rejected on the next op; non-expired and NULL-expiry allowed.
- Existing full-access tokens (NULL/empty new fields) behave exactly as before — regression.

## 3. ENH-003 — Webhook management (full: CRUD + edit + delivery status)

### 3.1 Server additions

Existing surface is 3 routes (`admin.rs:1579-1583`): `GET`/`POST /admin/db/{db}/webhooks`, `DELETE /admin/db/{db}/webhooks/{id}`. Add:

- `PUT /admin/db/{db}/webhooks/{id}` — edit `url` / `table` / `events` / `enabled`. New `enabled BOOLEAN NOT NULL DEFAULT true` column on `rtdb.webhooks` (`db.rs:216`); the delivery worker skips disabled rows at `enqueue_for_ops` (`webhook.rs:124`). (Disable-without-delete is the obvious operator need and is cheap once `PUT` exists.)
- `GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=` — paginated read of the `rtdb.webhook_deliveries` outbox (`db.rs:233`) with optional `status` filter (`pending|retrying|delivered|failed`). Returns `{ deliveries: [{ id, attempts, status, nextAttempt, lastError, payload }] }`. Admin-only.

Both gated by `require_admin` and by `Config::webhooks_enabled` (no-op when disabled, matching existing handlers).

### 3.2 Clients & dashboard

Add `listWebhooks / createWebhook / editWebhook / deleteWebhook / listDeliveries` to ts-client `RtDbAdminClient`, dashboard `AdminClient`, rust-client admin, and python admin. Template: the `listSchedules/createSchedule/cancelSchedule` methods (`dashboard/src/lib/admin.tsx:200-225`) for the per-db + `/{id}` shape.

**Webhooks page** (`dashboard/src/pages/WebhooksPage.tsx`): db selector, list (url/table/events/enabled badges), create + edit forms, delete, and a per-webhook delivery-status drill-down (recent deliveries with status/attempts/lastError). Co-located CSS module; route + nav registration.

### 3.3 Tests

- Create → list → edit (change url/events, toggle enabled) → delete round-trip.
- Disabled webhook produces no deliveries on a write.
- Deliveries endpoint returns attempted/failed/delivered rows filtered by status, paginated.

## 4. ENH-004 — Audit-log page (filterable)

### 4.1 Server

Extend `GET /admin/audit` (`admin.rs:1393`) `AuditParams` (`admin.rs:1367`) with optional `table`, `op`, `principal`, `source` filters (today only `db` is supported). Thread them into `fetch_audit_rows` (`audit.rs:99`) as parameterized `WHERE` clauses: `AND ($table::text IS NULL OR tbl = $table)`, etc. `limit`/`offset` clamping unchanged (`[1,1000]`). Admin-only; short-circuits to empty when `audit_log_enabled` is false.

### 4.2 Clients & dashboard

Add `getAudit({ db?, table?, op?, principal?, source?, limit?, offset? })` to ts-client, dashboard, rust-client, python. **Audit page** (`dashboard/src/pages/AuditPage.tsx`): filter bar (db/table/op/principal/source) + paginated table. Add `formatDateTime(ms)` to `dashboard/src/lib/format.ts` (no absolute date+time formatter exists today — pages inline one each).

### 4.3 Tests

- Filter combinations (e.g. db + op) return only matching rows; empty filter returns newest-first as today.
- Disabled audit (`audit_log_enabled=false`) returns empty.

## 5. ENH-010 — Live subscription inspector (+ per-db counters)

### 5.1 Per-db counters (the metrics change)

`Metrics` (`metrics.rs:101`) today holds only global `AtomicU64` counters. Add a per-db layer: a `DashMap<String, DbSubCounters>` (per-db atomic skips-by-class / reruns / missed) updated alongside the global counters inside `fan_out` (`subs.rs:980-994`), where the db is already known. `record_subs_skip` / `record_subs_rerun` / `record_subs_missed` gain a `db` argument and update both global and per-db. **Global counters are unchanged** — the Prometheus scrape (`/metrics`, `metrics.rs:304-346`) is byte-identical; per-db counters are exposed only in the JSON metrics snapshot and the inspector endpoint, avoiding label cardinality on the public scrape.

### 5.2 Inspector endpoint

`GET /admin/subscriptions?db=` (admin-only, optional db filter). Snapshots the registry using the sanctioned `count()` lock pattern (`subs.rs:878`): clone shard `Arc`s under the outer map lock, drop the outer lock, then lock shards one at a time — **never** hold the outer lock while waiting on a shard (the lock-ordering rule at `subs.rs:768-776`). This does not touch the single-writer invariant (that is about who commits DB writes; the registry is a shared map behind async mutexes, and `count()` already proves an external read path).

Returns, per active subscription, the metadata already in memory: `{ db, table, terminal, readSetClass, principal? }` (from `SubEntry` at `subs.rs:740`). There is **no per-sub subscriber count** (the registry keys on `(ConnId, queryId)`; each entry is one subscriber's one query) and **no per-sub skip/run counter** — the response is explicit about this and surfaces the new per-db counters and the global counters instead.

### 5.3 Clients & dashboard

Method on all four clients. **Subscriptions page** (`dashboard/src/pages/SubscriptionsPage.tsx`): db selector, table of active subscriptions (table/terminal/read-set class/principal), and a counters panel (global + per-db skip/re-run/missed).

### 5.4 Tests

- Register subscriptions across two dbs, mutate, assert the inspector lists them with correct table/terminal/read-set class and the per-db counters incremented appropriately.
- Snapshot does not deadlock under concurrent fan_out (the lock-ordering rule).

## 6. Python admin client (new — full parity)

`python-client` ships no admin surface today. Create `src/par_rt_db/admin.py` with a sync `httpx`-based `RtDbAdminClient` and an async variant under the `[aio]` extra, mirroring the ts-client `RtDbAdminClient` method set:

- **Pre-existing methods** (true parity): db lifecycle (`create_db`/`delete_db`/`list_dbs`), `push_schema`/`get_schema`, allowlist + admins CRUD, login/logout, export/import, `db_stats`, `metrics`, get/patch config, `ops_recent`, `admin_query`/`admin_mutate`, `migrate`, backup lifecycle.
- **New methods** added as each enhancement lands: token mint/revoke/list (new shape), webhook CRUD + deliveries, audit, subscriptions.

Bootstrapped as part of ENH-005 (with the new token shapes so no rework), filled out across the other units, and completed by a final parity sweep for any remaining legacy methods. Type-annotated (pyright) and ruff-clean, matching the existing python-client style.

## 7. Sequencing & branches

Five units, each its own feature branch (per `ENHANCEMENTS.md`), lightest-dependency first:

1. **ENH-005 tokens** (+ python admin bootstrap) — foundational; touches auth core + wire.
2. **ENH-003 webhooks** (`PUT` + deliveries + `enabled`).
3. **ENH-004 audit filters**.
4. **ENH-010 subscriptions** (per-db counters + inspector).
5. **Python admin parity sweep** (remaining legacy methods).

Within each unit: **server first** (contract source of truth), then the **client mirrors in parallel** (ts / rust / python are independent files), then the **dashboard page**. Each unit is committed atomically and gated on `make checkall` before its checkbox flips.

## 8. Verification & invariants

- **Gate**: `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) per unit, with `make dev-db-up` (dev Postgres on `127.0.0.1:55434`) — integration tests hit a real DB.
- **Wire contract**: the four protocol implementations (`server/protocol.rs` + `admin.rs`, `ts-client`, `rust-client`, `python-client`) stay byte-identical; serde tags/field names match exactly. The dashboard `lib/types.ts` mirrors the admin shapes.
- **Single-writer invariant**: no new writer. The subscription inspector only *reads* the registry (behind async mutexes); it never executes txns outside the committer. Webhook/audit/storage writes go through their existing paths.
- **SQL safety**: every new identifier is double-quoted; every value is `$n`-bound; no interpolation.
- **Errors**: every new failure is an `RtDbError { code, message }` envelope; client-facing 500s stay generic.
- **Docs**: `FEATURE_MATRIX.md` rows flip where parity is affected; each `ENHANCEMENTS.md` checkbox flips only after its gate passes; relevant README/skill notes update.
- **No `unwrap`/`expect` outside `#[cfg(test)]`**; zero clippy warnings.

## 9. Out of scope / follow-ups

- Per-subscription (not per-db) skip/run counters — would require per-sub state on `SubEntry`; deferred.
- Webhook signing secrets (`Webhook.secret`) — not in the ENH-003 body; deferred.
- `ownerField`-style row scoping inside the `tables` allowlist — tokens are table-level only.
- Python client reactive-WS admin parity is already shipped; this adds only the HTTP-admin surface.
