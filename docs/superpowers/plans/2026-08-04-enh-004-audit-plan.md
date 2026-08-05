# ENH-004 Audit-log Dashboard Page (filterable) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the audit trail reviewable from the operator console — extend `GET /admin/audit` with `table`/`op`/`principal`/`source` filters (today it filters by `db` only), mirror a `getAudit` method across all four clients, and build a filterable, paginated dashboard `AuditPage`.

**Architecture:** Server-first: add four optional filter params to `AuditParams` and thread them into `fetch_audit_rows` as parameterized `WHERE` clauses (nullable-filter shape, same as the webhooks deliveries query). Then a `getAudit` client method (ts/rust/python) and a dashboard `AuditPage` with a filter bar + paginated table + a shared absolute-timestamp formatter.

**Tech Stack:** Rust (axum/sqlx), TypeScript (bun/vite React), Python (httpx/pyright), Postgres 17.

## Global Constraints

- **Wire contract byte-identical**, camelCase. The `AuditEntry` response shape is unchanged (`{ id, tsMs, db, table, op, docId, principal, source }`); only new optional query params are added.
- **No new DB column / no schema change** — filtering is over the existing `rtdb.audit_log` columns (`db, tbl, op, doc_id, principal, source`).
- **SQL safety**: nullable-filter pattern `AND ($col::text IS NULL OR <col> = $col)`, every value `$n`-bound, no interpolation. `limit` clamped `[1,1000]`, `offset ≥ 0`.
- **Errors**: admin-only (`require_admin`); short-circuits to empty when `Config::audit_log_enabled` is false (existing behavior — preserve it).
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings (`-D warnings`).
- **Gate**: `make checkall` green before the ENH-004 checkbox flips. Tests need `make dev-db-up` (`127.0.0.1:55434`).

**Branch:** `enh-004-audit` (in-place, off `main`/`6c8e5c1`).

---

## File Structure

**Server:** `server/src/admin.rs` (`AuditParams` ~`:1367`, `audit_recent` handler ~`:1393`), `server/src/audit.rs` (`fetch_audit_rows` ~`:99`, `AuditEntry` ~`:62`), `server/tests/` (admin/audit test).
**ts-client:** `ts-client/src/admin.ts` (`getAudit` + types), `ts-client/tests/admin.test.ts`.
**rust-client:** `rust-client/src/wire.rs` + admin method, `rust-client/tests/`.
**python-client:** `python-client/src/par_rt_db/admin.py` (`get_audit` on both clients), `python-client/tests/test_admin.py`.
**dashboard:** `dashboard/src/lib/format.ts` (`formatDateTime`), `dashboard/src/lib/{types,admin}.tsx`, `dashboard/src/pages/AuditPage.tsx` (+`.module.css`), `App.tsx`, `shell/AppShell.tsx`.
**Docs:** `ENHANCEMENTS.md` (flip ENH-004).

---

## Task 1: Server — audit filter params

**Files:** `server/src/admin.rs`, `server/src/audit.rs`, `server/tests/`.
**Interfaces:** `AuditParams { db?, table?, op?, principal?, source?, limit, offset }`; `fetch_audit_rows(pool, db, table, op, principal, source, limit, offset)`; response unchanged.

- [ ] **Failing test**: seed `rtdb.audit_log` rows (or trigger real audit writes with `RTDB_AUDIT_LOG_ENABLED`), then `GET /admin/audit?db=&table=&op=&principal=&source=` returns only matching rows; combos filter correctly; absent filters return newest-first as today; disabled → empty.
- [ ] Run → FAIL.
- [ ] Extend `AuditParams` with `table/op/principal/source: Option<String>` (`#[serde(default)]`).
- [ ] Thread into `fetch_audit_rows`: nullable-filter `AND ($2::text IS NULL OR tbl = $2) AND ($3::text IS NULL OR op = $3) AND ($4::text IS NULL OR principal = $4) AND ($5::text IS NULL OR source = $5)` (note the column is `tbl`, not `table`).
- [ ] Run → PASS; `cargo fmt && cargo clippy --all-targets -- -D warnings`.
- [ ] Commit `feat(server): audit endpoint gains table/op/principal/source filters (ENH-004)`.

## Task 2: ts-client mirror
- [ ] `getAudit(opts?: { db?, table?, op?, principal?, source?, limit?, offset? })` building the query; `AuditEntry` type. Test the query + row parse. `cd ts-client && bunx tsc --noEmit && bunx vitest run tests/admin.test.ts`. Commit `feat(ts-client): getAudit method (ENH-004)`.

## Task 3: rust-client mirror
- [ ] `AuditEntry` wire struct (camelCase, `#[serde(default)]`) + `get_audit(db, opts)` method. Strict wire-shape tests + legacy back-compat. `cd rust-client && cargo test --all-features && cargo clippy -- -D warnings`. Commit `feat(rust-client): get_audit method (ENH-004)`.

## Task 4: python-client mirror
- [ ] `AuditEntry` model (import from http_client per F2a pattern, or define) + `get_audit(db, *, table=None, op=None, principal=None, source=None, limit=None, offset=None)` on both clients. MockTransport query test. `cd python-client && uv run pyright && uv run ruff check . && uv run pytest -q`. Commit `feat(python-client): get_audit method (ENH-004)`.

## Task 5: dashboard AuditPage
- [ ] `formatDateTime(ms)` in `dashboard/src/lib/format.ts` (no absolute date+time formatter exists). `AuditEntry` type + `AdminClient.getAudit`. `AuditPage.tsx`: db selector + filter bar (table/op/principal/source dropdowns/inputs) + paginated table (tsMs formatted, db/table/op/principal/source/docId). Route + nav. Optionally `AuditPage.test.tsx`. `make ts-client-build && cd dashboard && bunx tsc --noEmit && bun run build && bun run test`. Commit `feat(dashboard): Audit page with filters (ENH-004)`.

## Task 6: docs + full gate + close ENH-004
- [ ] `make dev-db-up && make checkall` green. Flip ENH-004 `[ ]`→`[x]`. Commit `docs: ENH-004 complete`.

---

## Self-Review (completed)
- **Spec coverage:** spec §4.1 (filter params) → Task 1; §4.2 clients → Tasks 2–4; §4.2 dashboard + formatDateTime → Task 5; verification → Task 6.
- **No placeholders;** test contracts concrete (helper names adapt to existing binaries). Note `rtdb.audit_log` column is `tbl` (renamed to `table` on the wire by `AuditEntry`).
- **Scope:** single unit (ENH-004). No new audit-write surface; read-only filters.
