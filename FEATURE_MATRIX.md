# Feature Matrix — Convex vs par-rt-db

**Date:** 2026-07-21 (gap matrix last updated 2026-07-23)
**Purpose:** Inventory Convex's feature surface against par-rt-db's, and rank every gap
by utility and level of effort so parity work can be picked off in value order.
**Perspective:** "Utility" is judged for the apps this instance actually serves (kanban
board, personal SPAs, CLI/agent tooling) — not for a hypothetical SaaS at scale.
**Sources:** `docs/superpowers/specs/2026-07-21-par-rt-db-design.md`, the implemented
server (`server/src/`), the client SDK (`ts-client/src/`), and Convex's documented feature
set (queries/mutations/actions, scheduling, storage, search, auth, components).

## Legend

| Symbol | Meaning |
|---|---|
| ✅ | Implemented / at parity |
| 🟡 | Partial |
| ❌ | Missing |
| 🚫 | Deliberate non-goal (architecture decision, not a gap) |

**Utility** — High: most apps built on par-rt-db would use it. Medium: some apps, or a
meaningful DX/reliability improvement. Low: rarely needed at this scale.
**Effort** — S: ≤1 day. M: 1–3 days. L: about a week. XL: multi-week / needs its own spec.

## 1. At parity today

| Capability | Convex | par-rt-db | Notes |
|---|---|---|---|
| Reactive live queries | ✅ | ✅ | WS `/sync`, push-on-change only, diffed via canonical JSON (`committer.rs`, `subs.rs`) |
| Atomic multi-step mutations | ✅ server TS functions | ✅ declarative txn DSL | `insert`/`patch`/`delete`/`upsert` + `expectVersion`/`expectAbsent` preconditions |
| Typed schema, core validators | ✅ | ✅ | `string, number, boolean, null, id, literal, optional, union, array, object` |
| Secondary + compound indexes | ✅ | ✅ | Real Postgres btree per index; `_creationTime` tiebreaker matches Convex ordering |
| Query surface: `get`, index-prefix `eq`, `order`, `take`, `collect`, `unique` | ✅ | ✅ | Semantics mirror Convex exactly (`query.rs`) |
| System fields | ✅ `_id`, `_creationTime` | ✅ + `_version` | `_version` is extra — it powers client-side OCC that Convex doesn't expose |
| End-to-end TypeScript types | ✅ via codegen | ✅ **no codegen** | Schema is TS; `Doc`/`Id` inferred (`schema.ts`) |
| React bindings | ✅ | ✅ | `RtDbProvider`, `useQuery` (undefined-until-first-result, `"skip"`), `useMutation`, auth gates |
| Client resilience | ✅ | ✅ | Auto-reconnect, re-auth, resubscribe, heartbeat, stale-callback generation guard |
| One-shot HTTP for machines | ✅ (fetchQuery / HTTP client) | ✅ | `POST /api/query` / `/api/mutate` with machine tokens |
| User auth | ✅ Clerk/Auth0/custom JWT/Convex Auth | 🟡 GitHub + Google OAuth + sessions | Provider trait (`auth/provider.rs`) — GitHub + Google today, each extra provider is small; cross-provider same-email logins link by email; per-database email allowlist replaces per-function auth checks |
| Live permission revocation | ✅ | ✅ | `authorize` re-runs on every Subscribe/Mutate; machine-token revocation, allowlist removal, and session expiry are all checked live per op (row 8 below) |
| Multi-app hosting | ✅ projects/deployments | ✅ named databases | One instance, many DBs — lighter than Convex's per-deployment model |
| Typed error envelope | ✅ | ✅ | `{code, message}`, seven codes, both transports |
| Schema migration on push | ✅ | 🟡 additive-only | Destructive pushes rejected by design; Convex allows more (with backfill) |
| Admin control plane | ✅ dashboard + CLI | ✅ HTTP + `admin.ts` (TS) + admin methods (Rust client) | create-db, push-schema, mint/revoke tokens, allowlist, export/import |

## 2. Gap matrix — ranked by utility ÷ effort

Rank 1 is the best next investment. Tier 1 = quick wins, Tier 2 = medium builds with
high leverage, Tier 3 = large projects.

| # | Tier | Feature | Convex | par-rt-db | Utility | Effort | Implementation sketch |
|---|---|---|---|---|---|---|---|
| 1 | 1 | Index **range queries** (`gt/gte/lt/lte` after the `eq` prefix) | ✅ | ✅ | High | M | Implemented — `Query` carries optional `gt`/`gte`/`lt`/`lte` bounds on the index field immediately after the `eq` prefix, typed via the existing `eq_binds`/`eq_bind_for` conversion in `txn.rs` (no forked typing). Mirrored end-to-end: `protocol.rs`/`protocol.ts` wire shape and `TableQuery.gt()/.gte()/.lt()/.lte()` in the TS client, with integration coverage in `query_test.rs` and `query.test.ts`. |
| 2 | 1 | **`.first()`** terminal | ✅ | ✅ | Med | S | Implemented — sugar over `take(1)`: `first: bool` on `Query`, mutually exclusive with `take`/`unique` (and with `get`, like the other terminals), returns `Doc(Some)`/`Doc(None)` instead of `Docs`. Mirrored end-to-end: `protocol.ts` wire shape and `TableQuery.first()` in the TS client, with integration coverage in `query_test.rs` and builder-shape coverage in `query.test.ts`. |
| 3 | 1 | **`count()`** terminal | 🟡 (needs aggregate component) | ✅ | Med | S | Implemented — `count: bool` on `Query`, a terminal running `SELECT COUNT(*)` over the same eq-prefix + range-bound WHERE clause every other terminal builds, uncapped by the 4096-row take limit; mutually exclusive with `get`/`take`/`unique`/`first`/`order` the same way `first` is. `QueryResult` gains an untagged `Count(i64)` variant, so it flows as a plain JSON number over both one-shot HTTP and the reactive WS query with no special-casing. Mirrored end-to-end: `protocol.ts` wire shape and `TableQuery.count()` in the TS client, with integration coverage in `query_test.rs` and builder-shape coverage in `query.test.ts`. Postgres makes this free where Convex needs a sharded-counter component — we exceed Convex here. |
| 4 | 2 | **Safe mutation retry** (idempotency keys) | ✅ auto-retry, exactly-once | ✅ | High | M | Implemented — a per-db `mutations(mut_id, result, expires_at)` table (sibling of the existing `meta` table), checked and stored inside the single per-db committer task so a retry with the same key replays the cached result instead of re-executing (`mutation_log.rs`, `committer.rs`). TTL is a fixed 5 minutes. Deliberately opt-in, not automatic: `client.ts`'s reject-on-close behavior is unchanged, and callers retry by supplying the same id again via the client's public option — `RtDbClient.mutate(txn, {mutId})` and `RtDbHttpClient.mutate(txn, {mutId})`. On the wire this travels as a separate, genuinely optional `idempotencyKey` field on both transports — WS's pre-existing, always-sent `mutId` stays pure reply-correlation and is never persisted, so a default call with no options never touches the dedup table at all. Mirrored end-to-end: HTTP's `MutateRequest` gains an additive optional `idempotencyKey` field, with integration coverage in `mutation_dedup_test.rs`, `ws_test.rs`, `http_api_test.rs`, `subs_test.rs`, and passthrough coverage in `client.test.ts`/`http.test.ts`. |
| 5 | 2 | **Pagination** (cursor + `usePaginatedQuery`) | ✅ | ✅ | High | M–L | Implemented — keyset pagination via an opaque base64 cursor encoding the full sort-column tuple (every unbound index field after the `eq` prefix, plus `created_at` and `id` as tie-breakers), so the server resumes strictly *after* the last row on the previous page via a standard OR-of-AND row-value predicate; `id` is globally unique so pages never skip or duplicate rows. Server half: a `paginate: Option<{cursor, numItems}>` terminal on `Query` (`query.rs`) returns `QueryResult::Paginated({docs, nextCursor})`, composes with `index`/`eq`/range bounds (`gt`–`lte`)/`order`, fetches `numItems+1` to detect the next page, and omits `nextCursor` (not null) when there is none. Both structs carry `rename_all = "camelCase"` and `next_cursor` uses `skip_serializing_if` so the wire stays camelCase-identical to the rest of the protocol. Client half: `TableQuery.paginate(cursor, numItems)` builder (typed `RtQuery<PaginatedResultJson>`), `encodeCursor`/`decodeCursor` helpers (`pagination.ts`), and a reactive `usePaginatedQuery` hook (`usePaginatedQuery.tsx`, exported from `./react`) that keeps each loaded page as a live `client.subscribe()` subscription, stitches docs across pages, exposes `loadMore`/`refetch`/`hasNextPage`, and tears down stale subs before creating new ones (the subscribe surface replays cached values, so order matters). `canonical()` serializes the `Paginated` variant so subscription diffing in the committer works for paginated queries. Mirrored end-to-end with integration coverage in `query_test.rs` (13 tests incl. a no-gaps/no-dupes full walk, compound-index cursor round-trip, DESC, and a paginate+`gte` range-bound walk), builder+codec coverage in `query.test.ts`, hook coverage in `react.test.tsx`, and an opt-in E2E round trip in `tests/integration/pagination.test.ts`. |
| 6 | 1 | **`replace`** step (full-document overwrite) | ✅ | ✅ | Med | S | Implemented — a `Step::Replace { table, id, doc }` variant in `txn.rs`: like `Insert`, the full document is validated against the schema and every indexed `f_<field>` column is recomputed from it (not merged like `Patch`), plus the row's `version` is bumped; `NotFound` if `id` doesn't exist. Mirrored end-to-end: `protocol.ts` wire shape and `TxnBuilder.replace()` in the TS client, with integration coverage in `txn_test.rs` and builder-shape coverage in `mutation.test.ts`. |
| 7 | 1 | **Snapshot export / import** per database | ✅ | ✅ | Med | S–M | Implemented — `GET /admin/export-db?db=` (`snapshot::export_database`) renders the pushed schema plus every document across every table as JSONL (a `{"kind":"schema"}` line, then one `{"kind":"doc"}` line per document carrying its `id`/`doc`/`createdAt`/`version`, tables and rows in stable order); `POST /admin/import-db?db=` (`snapshot::import_database`) applies the schema line through the existing `ddl::push_schema` and replays each doc line with its original id/timestamp/version preserved, recomputing indexed columns the same way `txn::do_insert` does. Both routes reuse `admin::require_admin`'s constant-time key check unchanged — no new auth mechanism. Mirrored end-to-end: `RtDbAdminClient.exportDb()/importDb()` in the TS client, with integration coverage in `admin_test.rs` (export→import round trip, unauthorized access, empty-database export). Complements host-level `pg_dump` with app-level portability (seed data, clone-to-dev). |
| 8 | 2 | **Live session-expiry enforcement** on open WS | ✅ | ✅ | Med | S | Implemented — `Principal::User` carries the session's `expires_at` (captured once at session resolution in `session.rs`), and `authorize` checks it before the allowlist query on every Subscribe/Mutate, rejecting with `UNAUTHORIZED` while leaving the connection open for retry with a fresh token; no extra DB round-trip because a session's expiry is immutable once minted. Integration coverage in `oauth_test.rs` (mid-connection expiry over an open WS denies subscribe and mutate but keeps the connection usable) and `http_api_test.rs` (direct `authorize` rejection of an expired, allowlisted principal). |
| 9 | 2 | **Scheduled transactions** (`runAfter`/`runAt` analog) | ✅ schedules functions | ✅ | Med–High | M | Implemented — a per-db `scheduled_txns` side table (sibling of `mutations`, created in `db::create_database` and lazily by `scheduler::ensure_table`) stores `(due_at, txn)` rows of declarative `Transaction`s, not code. A per-db scheduler timer (`scheduler::run_scheduler`, spawned alongside the committer in `Committers::channel_for`) claims due rows (`claim_due` with `FOR UPDATE SKIP LOCKED`, bounded by `CLAIM_BATCH`) and enqueues each as a fire-and-forget `CommitterRequest::RunScheduled`; the committer's `RunScheduled` arm (`committer.rs::handle_scheduled`) executes it via the normal `execute_txn` + `subs.fan_out` path and finalizes the row, so the single-writer invariant is intact. `when` is `afterMs`/`runAt` (one-shot); a past `runAt` fires immediately (catch-up). **At-least-once**: a crash between commit and finalize is recovered by `reset_running` on startup, so apps should write idempotent scheduled txns. Surface: WS `Schedule`/`CancelSchedule`/`PauseSchedule`/`ResumeSchedule`/`ListSchedules` and HTTP `POST /api/schedule`, `POST /api/schedule/{id}/{cancel,pause,resume}`, `POST /api/schedules`, with auth re-run on every WS op. Mirrored end-to-end: `ts-client` and `rust-client` ship `schedule`/`cancelSchedule`/`pauseSchedule`/`resumeSchedule`/`listSchedules` (reactive WS + one-shot HTTP), and the TS in-memory test harness adds a timer-less `tick()`. |
| 10 | 2 | **Cron jobs** (recurring txns) | ✅ | ✅ | Med | S (after #9) | Implemented — recurrence rides the same `scheduled_txns` table and scheduler as #9: `when: {type:"cron", expr}` validates a 5-field standard Vixie-cron expression (**min-first**, UTC, via the `croner` crate) and sets the first `due_at`. After each successful fire, `handle_scheduled` recomputes the next fire (`scheduler::next_fire` → `finalize_cron_next`) and bumps `fired_count`; a failed cron fire reschedules to the next window and records `last_error` but keeps firing (unlike a failed one-shot, which goes `status='error'` and stops). Cron **skips** missed windows — no backfill: if the server is down across a fire time, that window is lost, not caught up. Same WS/HTTP/client surface as #9 (`cron` is just another `ScheduleWhen` variant). |
| 11 | 2 | **Full-text search** | ✅ search indexes | ✅ | Med | M | Implemented — a declared search index (`IndexDef` carries an additive `search: true` flag, omitted from the wire for ordinary btree indexes so existing schemas deserialize unchanged) compiles to a generated `tsvector` column (`to_tsvector('english', …)` over its text fields, coalesced for nulls) plus a GIN index on it; the `search` query terminal (`{index, query}` on `Query`, mutually exclusive with every other terminal) matches that tsvector against `plainto_tsquery($1)` and ranks by `ts_rank` DESC with `(created_at, id)` tie-breakers, composing with `take`. The query text is bound once via `$n` and reused in the `ORDER BY ts_rank`, so user text can never inject tsquery syntax; malformed search (unknown index, empty query) is a clear `BadRequest`. Mirrored end-to-end across all three clients — server, rust-client (`search_index()` schema declaration + `.search()` query builder), and ts-client (`searchIndex()` + `.search()`). Native Postgres fit: no external service, free where Convex needs a dedicated search-index component. |
| 12 | 2 | **Optimistic updates** | ✅ | ✅ | Med | M | Implemented (ts-client) — opt-in `optimisticUpdates` on `RtDbClient` overlays a projected effect on each subscription's last result synchronously on `mutate`, then reconciles to the authoritative `queryUpdate` (server wins) and rolls back on `mutateErr`/reject/close. Correctness over coverage: only unambiguous projections (insert/patch/delete on known result docs) overlay; everything else waits for the server. Pure coverage in `optimistic.test.ts`. Rust client pending. |
| 13 | 1 | Extra validators: `record`, `int64`, `any`, `bytes` | ✅ | ✅ | Low–Med | S–M | Implemented — four new `FieldType` variants (`schema.rs`): `record` (dynamic string-keyed map, each entry validated against its `value` validator), `any` (accepts and stores any JSON value with zero validation), `bytes` (a JSON string validated as standard base64 with required padding, RFC 4648 §4), and `int64` (a JSON string of canonical decimal digits validated via `i64::from_str` — chosen because JSON numbers are IEEE-754 doubles and cannot exactly represent the full `i64` range past `Number.MAX_SAFE_INTEGER`). None of the four get a DDL-indexed column — `record`/`any`/`bytes` aren't scalar-comparable, and `int64` is deliberately left non-indexable in this pass. Mirrored end-to-end: `protocol.ts`'s `FieldTypeJson` and the client's `t.record()/t.any()/t.bytes()/t.int64()` factories, with schema/DDL/round-trip coverage in `schema_validators_test.rs` and factory/type coverage in `schema.test.ts`/`schema.types.test.ts`. **`int64` wire convention:** decimal-string on the wire, typed as a branded `Int64` string (not a real `bigint`) on the TS client — the client is entirely schema-type-erased at runtime (no codegen, no marshaling for any existing validator), so a real `bigint` would need a `JSON.stringify` replacer on writes and schema-aware result marshaling on reads that no other type needs; `Int64` instead follows the same zero-runtime-cost branded-string pattern already used for `Id<TableName>`, with `toInt64()`/`fromInt64()` helpers for apps that want actual `bigint` arithmetic. |
| 14 | 2 | **Additional OAuth providers** (Google, etc.) | ✅ many via integrations | 🟡 GitHub + Google | Med | M | Implemented — `OAuthProvider` trait (`auth/provider.rs`) with GitHub (refactored, routes byte-identical) and Google providers; each extra provider is now S. Identity is email-keyed with cross-provider same-email linking (both providers verified the email); Google requires a verified email. More providers (GitLab, Microsoft, …) are each a small `provider.rs` impl. |
| 15 | 2 | **db-side `filter()`** expressions | ✅ (discouraged in favor of indexes) | ✅ | Med | M | Implemented (server + rust-client builder) — a tagged-enum predicate DSL (`eq`/`neq`/`gt`/`gte`/`lt`/`lte`/`in` + `and`/`or` combinators) compiled to a fully-parenthesized WHERE fragment with every identifier schema-validated + double-quoted and every value `$n`-bound; indexed fields use their typed column, others use jsonb extraction with a value-inferred cast. Composes with index/order/take/cursor/count; `get` rejects it; malformed → `BadRequest`, never a 500. Mirrored end-to-end across all three clients (server + rust-client + ts-client `.filter()` builders). Convex steers users to indexes, so this stays an opt-in terminal. |
| 16 | 3 | **File storage** | ✅ upload URLs, serving, metadata | ✅ | Med–High | L | Implemented — Postgres-native blobs (per the user's vendor-lock-to-Postgres steer: no disk/S3/object-store, no trait). A per-db `storage` table (`bytea`, TOAST-managed) holds each file's bytes + `{sha256, size, contentType, createdAt}`, and a global `rtdb.storage_index(id → db_name)` resolves the public serve URL to its owning database. Upload is `POST /api/storage/{db}` (bearer, raw body, `Content-Type`); the route disables axum's 2 MiB default and enforces `RTDB_MAX_FILE_SIZE` (default 50 MiB) via `axum::body::to_bytes`, rejecting overflow as `BadRequest`. Serve is both **public** `GET /storage/{id}` (no auth — anyone with the opaque uuid-v7 URL fetches it, Convex parity; revoke by delete) and **authed** `GET /api/storage/{db}/{id}` (caller-db-scoped). Plus `DELETE /api/storage/{db}/{id}` (idempotent, revokes the public URL) and `GET /api/storage/{db}/{id}/metadata`. Storage is **HTTP-only** (not reactive → no WS variants) and writes via `storage::put` directly, not the committer (blobs don't touch document tables or subscriptions). Mirrored on ts-client (`upload`/`deleteFile`/`getFileMetadata`/`getUrl`) and rust-client (`upload`/`delete_file`/`get_file_metadata`/`get_url`). |
| 17 | 3 | **Vector search** | ✅ | ❌ | Med | M–L | `pgvector` extension + vector field type + index + `vectorSearch` terminal. Cheap infra-wise; ranked lower because current apps don't need it yet. |
| 18 | 3 | **Data browser dashboard** | ✅ full dashboard | ❌ | Med | L | Read/write table browser SPA over the admin API (could itself run on par-rt-db). Convex's dashboard is a real DX advantage; `psql` is the stopgap. |
| 19 | 3 | **Client test harness** (in-memory fake) | ✅ `convex-test` | ✅ | Med | M | Implemented (ts-client) — `InMemoryRtDbClient` (`src/in_memory.ts`) mirrors the server's schema/query/txn/step-result semantics with no network and no Postgres: `pushSchema`, `query`, `mutate` (with `mutId` idempotency), `subscribe` (reactive), cursor keyset pagination, system fields merged at read time, atomic rollback on step failure. Reuses `protocol.ts` types. Deferred gaps (additive schema evolution) marked as TODOs. Pure coverage in `in_memory.test.ts`. |
| 20 | 3 | **Per-row authorization rules** | ✅ arbitrary code in functions | ❌ allowlist = full access | Med–High | XL | Needs a declarative rule DSL (e.g. owner-field match) enforced on query, mutate, *and* subscription re-run. Only matters for multi-user apps where users must not see each other's rows — the kanban model doesn't. Deserves its own spec when needed. |
| 21 | 3 | **Fine-grained subscription invalidation** | ✅ read-set tracking | 🟡 table-level | Low (now) | L | Correct today, just coarse. Becomes worthwhile only with many subscriptions × high write rate. Contained inside `subs.rs`/`committer.rs` per the spec — no protocol change. |

## 3. Deliberate non-goals (not gaps)

These are consequences of the founding decision — **no embedded JS runtime, no per-app
server code** — or of being self-hosted rather than a cloud platform. Listed so nobody
mistakes them for backlog.

| Convex feature | Why it's out | par-rt-db-native alternative |
|---|---|---|
| **Actions** (server-side side effects) | Requires running user code (V8) | External worker with a machine token: HTTP query → do side effect → HTTP mutate. Scheduled txns (#9) cover the deferred-write half. |
| **HTTP actions** (custom endpoints) | Same — user code | Put the endpoint in the app's own host (workers, edge functions) calling the HTTP API. |
| Internal vs public **functions** | No functions at all — the DSL is the API | Machine tokens vs user sessions split trusted/untrusted callers. |
| **Components ecosystem** (workflow, agent, rate limiter, migrations…) | Components are packaged server code | Native terminals where Postgres makes it cheap (see `count`, search); otherwise external tooling. |
| Cloud platform features: preview deployments, env vars, log streams/integrations, streaming export (Fivetran/Airbyte) | Platform, not database | `docker compose` + `tracing` logs + direct Postgres access. |
| Multi-region / horizontal scale-out | Single-writer committer is load-bearing for correctness | Out of scope at personal-project scale by design. |

## 4. Where par-rt-db is ahead

- **No codegen** — schema *is* the TS source of types; no `npx convex dev` watcher, no
  generated `_generated/` directory to keep in sync.
- **Self-hosted and owned** — no vendor, no per-seat pricing, data in a Postgres you can
  `psql` into; nightly `pg_dump` backups.
- **SQL escape hatch** — anything the DSL can't express yet has a manual answer today.
- **`_version` on every doc** — explicit optimistic-concurrency primitive
  (`expectVersion`) that Convex doesn't surface.
- **Explicit at-most-once mutations** — no hidden auto-retry semantics (#4 adds
  opt-in safe retry without giving this up).
- **One instance, many databases** — a new app is an admin call, not a new deployment.

## 5. Recommended order

As of 2026-07-23, **done**: tier-1 **#1–#3, #6, #7, #13**; tier-2 **#4** (safe
retry), **#5** (cursor pagination), **#8** (session-expiry enforcement), **#9**
(scheduled txns), **#10** (cron jobs), **#11** (full-text search), **#12**
(optimistic updates), **#14** (OAuth provider trait + Google), **#15** (db-side
`filter()`); and tier-3 **#19** (in-memory client test harness), **#16** (file
storage). **#9/#10** landed as one per-db `scheduled_txns` side table drained
through the committer by a per-db scheduler timer (`scheduler.rs` + the
`RunScheduled` committer arm) — at-least-once delivery, one-shot catches up if
past due, cron skips missed windows. **#16** is a per-db `bytea` `storage`
table + global `storage_index` for opaque public-serve resolution — both public
(`GET /storage/{id}`, no auth) and authed serve, plus upload/delete/metadata;
HTTP-only (not reactive), Postgres-native by design. The Rust client is
feature-complete (`http` + reactive `ws` + `admin` + index/`mutate_with_retry`
helpers + `.filter()`/`.search()` builders + storage), and the TS client mirrors
it (`.filter()`/`.search()` + `searchIndex` + storage) — all three clients share
one wire contract for db-side `filter()` and full-text search, and the storage
HTTP surface.

Remaining gaps, in value order: **#17 (vector search)**, **#18 (data-browser
dashboard)**, and **#20 (per-row auth rules)**.

Client parity for db-side `filter()` and full-text search is now complete: the
TS `.filter()`/`.search()` query builders + `searchIndex` schema declaration
landed in commit 4a66c87, so both the ts-client and rust-client mirror the
server wire contract. No open client-parity follow-ups remain on those surfaces.
