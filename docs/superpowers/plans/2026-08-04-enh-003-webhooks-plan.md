# ENH-003 Webhook Management (full: CRUD + edit + delivery status) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the operator console a full webhook management page — create/list/edit/delete webhook subscriptions (toggling `enabled`) and inspect delivery attempts — by adding the missing server endpoints (`PUT` edit, `GET` deliveries, `enabled` column) and mirroring them across all four clients + the dashboard.

**Architecture:** Server-first (the contract source of truth): add an `enabled` column to `rtdb.webhooks`, a `PUT /admin/db/{db}/webhooks/{id}` edit route, and a `GET .../deliveries` read route over the existing outbox; the delivery worker skips disabled webhooks at enqueue. Then mirror the webhook client methods byte-identically across ts/rust/python and build the dashboard `WebhooksPage`.

**Tech Stack:** Rust (axum/sqlx), TypeScript (bun/vite React dashboard), Python (httpx/pyright), Postgres 17.

## Global Constraints

- **Wire contract byte-identical**, camelCase, across `server/src/{admin,webhook}.rs`, `ts-client/src/admin.ts`, `rust-client/src/`, `python-client/src/par_rt_db/admin.py`, `dashboard/src/lib/{types,admin}.tsx`. New fields optional where the server `#[serde(default)]`s them.
- **New DB column additive with safe default**: `enabled BOOLEAN NOT NULL DEFAULT true` on `rtdb.webhooks` (idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`). Existing webhooks stay enabled.
- **Single-writer invariant untouched**: webhook/delivery writes go through their existing paths; the delivery worker (`run_delivery_worker`) is unchanged in ownership — only `enqueue_for_ops` gains a skip-disabled check.
- **SQL safety**: double-quote identifiers, bind every value via `$n`, never interpolate. `fetch_optional` for lookups that can miss.
- **Errors**: every failure is `RtDbError { code, message }`. Edit/deliveries routes are admin-only (`require_admin`) and no-op-return-empty when `Config::webhooks_enabled` is false (matching existing handlers).
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings (`-D warnings`).
- **Gate**: `make checkall` must pass before the ENH-003 checkbox flips. Integration tests need `make dev-db-up` (`127.0.0.1:55434`).

**Branch:** `enh-003-webhooks` (in-place, off `main`/`ef4af7b`).

---

## File Structure

**Server (contract source of truth):**
- `server/src/db.rs` (~`:216-253` webhook tables ensure) — add `enabled` column via idempotent ALTER.
- `server/src/webhook.rs` (`Webhook` struct ~`:82-92`; `enqueue_for_ops` ~`:124-179`) — add `enabled` to `Webhook`; skip disabled rows at enqueue.
- `server/src/admin.rs` (~`:832-947` webhook handlers; routes ~`:1579-1583`) — add `PUT /admin/db/{db}/webhooks/{id}` (edit) + `GET /admin/db/{db}/webhooks/{id}/deliveries`; extend create/list to round-trip `enabled`.
- `server/tests/webhook_test.rs` (or the existing admin/webhook test binary) — new tests.

**ts-client:** `ts-client/src/admin.ts` (webhook methods + types), `ts-client/tests/admin.test.ts`.
**rust-client:** `rust-client/src/wire.rs` + admin methods, `rust-client/tests/`.
**python-client:** `python-client/src/par_rt_db/admin.py` (webhook methods on `RtDbAdminClient`/`AsyncRtDbAdminClient`), `python-client/tests/test_admin.py`.
**dashboard:** `dashboard/src/lib/types.ts` + `admin.tsx`, `dashboard/src/pages/WebhooksPage.tsx` (+ `.module.css`), `App.tsx`, `shell/AppShell.tsx`.
**Docs:** `ENHANCEMENTS.md` (flip ENH-003), `FEATURE_MATRIX.md` (webhook row if present).

---

## Task 1: Server — `enabled` column + delivery worker skip + Webhook struct

**Files:** `server/src/db.rs`, `server/src/webhook.rs`, `server/src/admin.rs` (list/create round-trip `enabled`), `server/tests/`.
**Interfaces:**
- Produces: `rtdb.webhooks.enabled BOOLEAN NOT NULL DEFAULT true`; `Webhook { ..., enabled: bool }` (serialized `enabled`); `enqueue_for_ops` skips rows where `enabled = false`; create accepts optional `enabled` (default true), list returns it.

- [ ] **Step 1: Failing test** — create a webhook, flip `enabled=false` directly (or via the edit endpoint once it exists in Task 2; for Task 1, set via SQL or create with `enabled:false`), trigger a matching doc op, assert **no** delivery row is enqueued for the disabled webhook (and a matching enabled webhook DOES get one).
- [ ] **Step 2: Run, confirm FAIL** (`cargo test --test <webhook binary>`).
- [ ] **Step 3: Add the column** — idempotent `ALTER TABLE rtdb.webhooks ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT true` in the webhook-tables ensure (`db.rs`), alongside the existing `CREATE TABLE IF NOT EXISTS rtdb.webhooks`.
- [ ] **Step 4: Extend `Webhook`** (`webhook.rs`) with `pub enabled: bool` (`#[serde(rename_all="camelCase")]` already on the struct → wire `enabled`). Extend the `list`/`create` SQL + the create-request struct (`enabled: Option<bool>` defaulting true) + the list SELECT to include `enabled`.
- [ ] **Step 5: Skip disabled at enqueue** — in `enqueue_for_ops` (`webhook.rs:124-179`), where matching webhooks are fetched for a db/table/event, filter out `enabled = false` rows (either in the matching SQL `WHERE enabled` or in-code before enqueue). Disabled webhooks produce no delivery rows.
- [ ] **Step 6: Run test → PASS**; `cargo fmt && cargo clippy --all-targets -- -D warnings`.
- [ ] **Step 7: Commit** — `feat(server): webhooks gain enabled flag + delivery worker skips disabled (ENH-003)`.

## Task 2: Server — `PUT` edit + `GET` deliveries endpoints

**Files:** `server/src/admin.rs`, `server/src/webhook.rs` (a `fetch_deliveries` helper), `server/tests/`.
**Interfaces:**
- Produces: `PUT /admin/db/{db}/webhooks/{id}` (body `{ url?, table?, events?, enabled? }` → updates the row, returns the updated `Webhook`); `GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=` → `{ deliveries: [{ id, attempts, status, nextAttempt, lastError, payload }] }`. Both admin-only; no-op/empty when `webhooks_enabled` is false.

- [ ] **Step 1: Failing tests** — (a) create a webhook, PUT to edit url/events + toggle `enabled=false`, GET the list, assert the updated fields; (b) seed a `webhook_deliveries` row, GET deliveries with `?status=` filter + pagination, assert the rows + counts.
- [ ] **Step 2: Run, confirm FAIL.**
- [ ] **Step 3: PUT edit handler** (`admin_edit_webhook`) — route `PUT /admin/db/{db}/webhooks/{id}`; `require_admin`; no-op 404/400 if `!webhooks_enabled`; body struct `AdminEditWebhookRequest { url: Option<String>, table: Option<Option<String>>, events: Option<Vec<String>>, enabled: Option<bool> }` (all optional; absent = unchanged — use `COALESCE`-style update or build the SET clause from present fields); `UPDATE rtdb.webhooks SET ... WHERE id=$1 AND db=$2` returning the updated row → `Webhook`. 404 if no row.
- [ ] **Step 4: GET deliveries handler** (`admin_list_deliveries`) — route `GET /admin/db/{db}/webhooks/{id}/deliveries`; params `status: Option<String>`, `limit` (default 50, clamp [1,1000]), `offset` (≥0); `require_admin`; empty when disabled; `SELECT id, attempts, status, next_attempt, last_error, payload FROM rtdb.webhook_deliveries WHERE webhook_id=$1 AND ($status::text IS NULL OR status=$status) ORDER BY next_attempt DESC LIMIT $l OFFSET $o`; response `{ deliveries: DeliveryRow{...} }` (`#[serde(rename_all="camelCase")]` → `nextAttempt`/`lastError`).
- [ ] **Step 5: Register routes** in `admin_routes()` (`.route("/admin/db/{db}/webhooks/{id}", put(admin_edit_webhook))` and `.route("/admin/db/{db}/webhooks/{id}/deliveries", get(admin_list_deliveries))`).
- [ ] **Step 6: Run tests → PASS**; `cargo fmt && cargo clippy --all-targets -- -D warnings`; `cargo test` (full server suite).
- [ ] **Step 7: Commit** — `feat(server): webhook edit (PUT) + deliveries (GET) endpoints (ENH-003)`.

## Task 3: ts-client mirror

**Files:** `ts-client/src/admin.ts`, `ts-client/tests/admin.test.ts`.
**Interfaces:** `Webhook`/`WebhookDelivery` types + `listWebhooks(db)`, `createWebhook(db, {url, table?, events?, enabled?})`, `editWebhook(db, id, {url?, table?, events?, enabled?})`, `deleteWebhook(db, id)`, `listDeliveries(db, id, {status?, limit?, offset?})`.
- [ ] TDD: types + methods (camelCase wire); test the request bodies + both row shapes. `cd ts-client && bunx vitest run tests/admin.test.ts && bunx tsc --noEmit && bunx biome check src/`. Commit `feat(ts-client): webhook management methods (ENH-003)`.

## Task 4: rust-client mirror

**Files:** `rust-client/src/wire.rs` + admin client methods, `rust-client/tests/`.
- [ ] TDD: `Webhook`/`WebhookDelivery` wire structs (camelCase, `#[serde(default)]` for back-compat) + `list_webhooks`/`create_webhook`/`edit_webhook`/`delete_webhook`/`list_deliveries` methods on the admin client. Strict `serde_json` wire-shape tests + legacy-fixture back-compat. `cd rust-client && cargo test --all-features && cargo clippy --all-targets --all-features -- -D warnings`. Commit `feat(rust-client): webhook management methods (ENH-003)`.

## Task 5: python-client mirror

**Files:** `python-client/src/par_rt_db/admin.py` (on `RtDbAdminClient`/`AsyncRtDbAdminClient`), `python-client/tests/test_admin.py`.
- [ ] TDD: `Webhook`/`WebhookDelivery` models (reuse/import pydantic models if shared, else define on admin) + `list_webhooks`/`create_webhook`/`edit_webhook`/`delete_webhook`/`list_deliveries` methods (sync + async). `MockTransport` body/route tests across categories. `cd python-client && uv run pyright && uv run ruff check . && uv run pytest -q`. Commit `feat(python-client): webhook management methods on admin client (ENH-003)`.

## Task 6: dashboard WebhooksPage

**Files:** `dashboard/src/lib/{types,admin}.tsx`, `dashboard/src/pages/WebhooksPage.tsx` (+`.module.css`), `App.tsx`, `shell/AppShell.tsx`, optionally `WebhooksPage.test.tsx`.
- [ ] Types (`Webhook`/`WebhookDelivery`) + `AdminClient` methods (`listWebhooks`/`createWebhook`/`editWebhook`/`deleteWebhook`/`listDeliveries`); `WebhooksPage` mirroring `ScheduledJobsPage` (db selector, list with enabled badge + edit, create form, delete confirm, per-webhook delivery-status drill-down). Route + nav. `make ts-client-build && cd dashboard && bunx tsc --noEmit && bun run build && bun run test`. Commit `feat(dashboard): Webhooks page with edit + delivery status (ENH-003)`.

## Task 7: docs + full gate + close ENH-003
- [ ] `make dev-db-up && make checkall` green (all 5 packages). Flip ENH-003 `[ ]`→`[x]` in `ENHANCEMENTS.md`; update `FEATURE_MATRIX.md` webhook row if present. Commit `docs: ENH-003 complete`.

---

## Self-Review (completed)
- **Spec coverage:** spec §3.1 (PUT edit + enabled + deliveries GET) → Tasks 1–2; §3.2 clients → Tasks 3–5; §3.2 dashboard → Task 6; verification → Task 7. All §3 requirements covered.
- **Placeholder scan:** none; test contracts are concrete (helper names adapt to the existing binaries).
- **Type consistency:** `Webhook.enabled: bool` / wire `enabled`; `DeliveryRow { nextAttempt, lastError }` camelCase consistent across tasks.
- **Scope:** single unit (ENH-003 full spec). Delivery-status read-only; no new delivery-mutation surface.
