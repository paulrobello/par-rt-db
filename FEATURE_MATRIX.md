# Feature Matrix — Convex vs par-rt-db

**Date:** 2026-07-21 (gap matrix last updated 2026-08-01; dimensional + divergence comparison added 2026-07-29)
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
| React bindings | ✅ | ✅ | `RtDbProvider`, `useQuery` (undefined-until-first-result, `"skip"`), `useMutation`, `useConnectionState`, auth gates |
| Client resilience | ✅ | ✅ | Auto-reconnect, re-auth, resubscribe, heartbeat, stale-callback generation guard; connection state observable via `getConnectionState`/`onConnectionChange` (ts) and `status`/`status_receiver` (rust) |
| One-shot HTTP for machines | ✅ (fetchQuery / HTTP client) | ✅ | `POST /api/query` / `/api/mutate` with machine tokens; `POST /api/query-batch` fans out N queries in one round trip (per-query error isolation) |
| User auth | ✅ Clerk/Auth0/custom JWT/Convex Auth | 🟡 GitHub + Google + GitLab + generic OIDC OAuth + sessions | Provider trait (`auth/provider.rs`) — GitHub + Google + GitLab + generic OIDC today, each extra provider is small; cross-provider same-email logins link by email; per-database email allowlist replaces per-function auth checks |
| Live permission revocation | ✅ | ✅ | `authorize` re-runs on every Subscribe/Mutate; machine-token revocation, allowlist removal, and session expiry are all checked live per op (row 8 below) |
| Multi-app hosting | ✅ projects/deployments | ✅ named databases | One instance, many DBs — lighter than Convex's per-deployment model |
| Typed error envelope | ✅ | ✅ | `{code, message}`, seven codes, both transports |
| Schema migration on push | ✅ | ✅ | Additive changes still apply automatically on push (new tables/fields/indexes, safe literal-union widening). Destructive/type-changing transformations are now applied via a **declarative migrate operation** (`POST /admin/db/{db}/migrate`, admin-only): an ordered `Directive` list — `renameField`/`renameTable` (no data loss), `changeType` (closed cast matrix `toString`/`toNumber`/`toInt64`/`toBoolean` + optional `default` choosing atomic-fail-vs-substitute, else a single un-coercible value rolls the whole migrate back), `dropField`/`dropTable`/`dropIndex`, `setDefault` (one-time backfill into existing rows), and a scoped `evalExpr` raw-SQL doc-rewrite escape (one table, `doc` jsonb only, no `FROM`/DDL verbs — the admin-trusted power op). Runs inside the committer's serialized turn as a third request arm (`RunMigrate`) so `fan_out` + op-feed + audit log + webhook outbox all fire; dry-run-first (returns a per-directive report + the derived resulting schema). HTTP-only (no WS peer, like `pushSchema`/export). Mirrored across all four clients (`Migration` builder + admin `migrate` method), the `rtdb migrate` CLI, and the dashboard (dry-run → review → apply). Spec: `docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md`. |
| Admin control plane | ✅ dashboard + CLI | ✅ HTTP + `admin.ts` (TS) + admin methods (Rust client) | `RtDbAdminClient` (TS) covers the full server admin HTTP surface — 25 methods: db create/delete/list/push-schema (`delete` requires a typed `confirm == name` guard), schema/stats read-back, token mint/revoke/list, db + server-wide admin allowlist CRUD, admin-key login/logout session, metrics, hot config GET/PATCH, op-feed `recent`, owner-bypass query/mutate, snapshot export/import. Deferred: `/admin/stream` (WS — structurally distinct, dashboard covers it). The Rust client (`RtDbHttpClient` under `feature = "admin"`) mirrors the machine-relevant subset — 22 methods (everything above except the cookie-session `login`/`logout` and the WS `/admin/stream`, which are browser-only); those plus an OAuth helper are excluded by design since the Rust client is a server-side machine client. |
| Observability | ✅ dashboard + log streams | ✅ `/metrics` + `/admin/metrics` + op feed | Prometheus text exposition on `/metrics` (content-negotiated so a browser gets the SPA) and the same snapshot as JSON on `/admin/metrics`: throughput counters, p50/p95/p99 latency, pool/WS/subscription gauges, `rtdb_build_info`. Plus **self-checking realtime invalidation**: `rtdb_subs_skips_total{class}` / `rtdb_subs_reruns_total` measure how much re-run work the read-set optimization avoids, and `RTDB_SUBS_VERIFY_SKIP_EVERY` (default off) shadow-verifies sampled skips so an under-approximation surfaces as `rtdb_subs_missed_pushes_total` + an ERROR log + a self-healing push instead of a silently dropped update (#21). |

## 2. Gap matrix — ranked by utility ÷ effort

Rank 1 is the best next investment. Tier 1 = quick wins, Tier 2 = medium builds with
high leverage, Tier 3 = large projects.

| # | Tier | Feature | Convex | par-rt-db | Utility | Effort | Implementation sketch |
|---|---|---|---|---|---|---|---|
| 1 | 1 | Index **range queries** (`gt/gte/lt/lte` after the `eq` prefix) | ✅ | ✅ | High | M | Implemented — `Query` carries optional `gt`/`gte`/`lt`/`lte` bounds on the index field immediately after the `eq` prefix, typed via the existing `eq_binds`/`eq_bind_for` conversion in `txn.rs` (no forked typing). Mirrored end-to-end: `protocol.rs`/`protocol.ts` wire shape and `TableQuery.gt()/.gte()/.lt()/.lte()` in the TS client, with integration coverage in `query_test.rs` and `query.test.ts`. |
| 2 | 1 | **`.first()`** terminal | ✅ | ✅ | Med | S | Implemented — sugar over `take(1)`: `first: bool` on `Query`, mutually exclusive with `take`/`unique` (and with `get`, like the other terminals), returns `Doc(Some)`/`Doc(None)` instead of `Docs`. Mirrored end-to-end: `protocol.ts` wire shape and `TableQuery.first()` in the TS client, with integration coverage in `query_test.rs` and builder-shape coverage in `query.test.ts`. |
| 3 | 1 | **`count()`** terminal | 🟡 (needs aggregate component) | ✅ | Med | S | Implemented — `count: bool` on `Query`, a terminal running `SELECT COUNT(*)` over the same eq-prefix + range-bound WHERE clause every other terminal builds, uncapped by the 4096-row take limit; mutually exclusive with `get`/`take`/`unique`/`first`/`order` the same way `first` is. `QueryResult` gains an untagged `Count(i64)` variant, so it flows as a plain JSON number over both one-shot HTTP and the reactive WS query with no special-casing. Mirrored end-to-end: `protocol.ts` wire shape and `TableQuery.count()` in the TS client, with integration coverage in `query_test.rs` and builder-shape coverage in `query.test.ts`. Postgres makes this free where Convex needs a sharded-counter component — we exceed Convex here. |
| 3b | 1 | **`distinct()`** terminal (unique values of the next index field) | 🟡 (no direct equivalent; `db.aggregate` is the nearest) | ✅ | Med | S | Implemented — `distinct: bool` on `Query`, a terminal running `SELECT DISTINCT to_jsonb("<col>")` over the index field immediately after the `eq` prefix (`index.fields[eq.len()]`), using the same eq-prefix + range-bound WHERE clause every other terminal builds (no duplicated SQL — the WHERE/column construction in `execute_query` is shared). Returns `QueryResult::Distinct(Vec<Value>)`, an untagged JSON array of those scalar values ordered ascending for deterministic output and capped by `MAX_TAKE` (4096). Useful for autocomplete/facet UIs. Requires both an `index` and an index field beyond the eq prefix → `BadRequest` otherwise. Mutually exclusive with every other terminal except `eq`/range bounds/`filter` (which narrow the matching set the distinct values are drawn from). Mirrored end-to-end across all four clients: server `Query`/`QueryResult` + `Peer::Distinct` added to every existing incompatible-peer table; ts-client `QueryJson.distinct`, `TableQuery.distinct()`, in-memory executor + cascade mirror; rust-client `Query.distinct` + `.distinct()` + `optimistic::is_array_query` exclusion; python-client `Query.distinct`, `TableQuery.distinct()`/`build_for_distinct()`, `parse_result("distinct", …)`. Cross-client combination-matrix tests (`server/tests/query_combinations.rs` + `ts-client/tests/query_combinations.test.ts`) cover the new terminal × every peer. Postgres makes this free; Convex needs a component or `db.aggregate` round-trip. |
| 4 | 2 | **Safe mutation retry** (idempotency keys) | ✅ auto-retry, exactly-once | ✅ | High | M | Implemented — a per-db `mutations(mut_id, result, expires_at)` table (sibling of the existing `meta` table), checked and stored inside the single per-db committer task so a retry with the same key replays the cached result instead of re-executing (`mutation_log.rs`, `committer.rs`). TTL is a fixed 5 minutes. Deliberately opt-in, not automatic: `client.ts`'s reject-on-close behavior is unchanged, and callers retry by supplying the same id again via the client's public option — `RtDbClient.mutate(txn, {mutId})` and `RtDbHttpClient.mutate(txn, {mutId})`. On the wire this travels as a separate, genuinely optional `idempotencyKey` field on both transports — WS's pre-existing, always-sent `mutId` stays pure reply-correlation and is never persisted, so a default call with no options never touches the dedup table at all. Mirrored end-to-end: HTTP's `MutateRequest` gains an additive optional `idempotencyKey` field, with integration coverage in `mutation_dedup_test.rs`, `ws_test.rs`, `http_api_test.rs`, `subs_test.rs`, and passthrough coverage in `client.test.ts`/`http.test.ts`. |
| 5 | 2 | **Pagination** (cursor + `usePaginatedQuery`) | ✅ | ✅ | High | M–L | Implemented — keyset pagination via an opaque base64 cursor encoding the full sort-column tuple (every unbound index field after the `eq` prefix, plus `created_at` and `id` as tie-breakers), so the server resumes strictly *after* the last row on the previous page via a standard OR-of-AND row-value predicate; `id` is globally unique so pages never skip or duplicate rows. Server half: a `paginate: Option<{cursor, numItems}>` terminal on `Query` (`query.rs`) returns `QueryResult::Paginated({docs, nextCursor})`, composes with `index`/`eq`/range bounds (`gt`–`lte`)/`order`, fetches `numItems+1` to detect the next page, and omits `nextCursor` (not null) when there is none. Both structs carry `rename_all = "camelCase"` and `next_cursor` uses `skip_serializing_if` so the wire stays camelCase-identical to the rest of the protocol. Client half: `TableQuery.paginate(cursor, numItems)` builder (typed `RtQuery<PaginatedResultJson>`), `encodeCursor`/`decodeCursor` helpers (`pagination.ts`), and a reactive `usePaginatedQuery` hook (`usePaginatedQuery.tsx`, exported from `./react`) that keeps each loaded page as a live `client.subscribe()` subscription, stitches docs across pages, exposes `loadMore`/`refetch`/`hasNextPage`, and tears down stale subs before creating new ones (the subscribe surface replays cached values, so order matters). `canonical()` serializes the `Paginated` variant so subscription diffing in the committer works for paginated queries. Mirrored end-to-end with integration coverage in `query_test.rs` (13 tests incl. a no-gaps/no-dupes full walk, compound-index cursor round-trip, DESC, and a paginate+`gte` range-bound walk), builder+codec coverage in `query.test.ts`, hook coverage in `react.test.tsx`, and an opt-in E2E round trip in `tests/integration/pagination.test.ts`. |
| 6 | 1 | **`replace`** step (full-document overwrite) | ✅ | ✅ | Med | S | Implemented — a `Step::Replace { table, id, doc }` variant in `txn.rs`: like `Insert`, the full document is validated against the schema and every indexed `f_<field>` column is recomputed from it (not merged like `Patch`), plus the row's `version` is bumped; `NotFound` if `id` doesn't exist. Mirrored end-to-end: `protocol.ts` wire shape and `TxnBuilder.replace()` in the TS client, with integration coverage in `txn_test.rs` and builder-shape coverage in `mutation.test.ts`. |
| 7 | 1 | **Snapshot export / import** per database | ✅ | ✅ | Med | S–M | Implemented — `GET /admin/export-db?db=` (`snapshot::export_database`) renders the pushed schema plus every document across every table as JSONL (a `{"kind":"schema"}` line, then one `{"kind":"doc"}` line per document carrying its `id`/`doc`/`createdAt`/`version`, tables and rows in stable order); `POST /admin/import-db?db=` (`snapshot::import_database`) applies the schema line through the existing `ddl::push_schema` and replays each doc line with its original id/timestamp/version preserved, recomputing indexed columns the same way `txn::do_insert` does. Both routes reuse `admin::require_admin`'s constant-time key check unchanged — no new auth mechanism. Mirrored end-to-end: `RtDbAdminClient.exportDb()/importDb()` in the TS client, with integration coverage in `admin_test.rs` (export→import round trip, unauthorized access, empty-database export). Complements host-level `pg_dump` with app-level portability (seed data, clone-to-dev). |
| 8 | 2 | **Live session-expiry enforcement** on open WS | ✅ | ✅ | Med | S | Implemented — `Principal::User` carries the session's `expires_at` (captured once at session resolution in `session.rs`), and `authorize` checks it before the allowlist query on every Subscribe/Mutate, rejecting with `UNAUTHORIZED` while leaving the connection open for retry with a fresh token; no extra DB round-trip because a session's expiry is immutable once minted. Integration coverage in `oauth_test.rs` (mid-connection expiry over an open WS denies subscribe and mutate but keeps the connection usable) and `http_api_test.rs` (direct `authorize` rejection of an expired, allowlisted principal). |
| 9 | 2 | **Scheduled transactions** (`runAfter`/`runAt` analog) | ✅ schedules functions | ✅ | Med–High | M | Implemented — a per-db `scheduled_txns` side table (sibling of `mutations`, created in `db::create_database` and lazily by `scheduler::ensure_table`) stores `(due_at, txn)` rows of declarative `Transaction`s, not code. A per-db scheduler timer (`scheduler::run_scheduler`, spawned alongside the committer in `Committers::channel_for`) claims due rows (`claim_due` with `FOR UPDATE SKIP LOCKED`, bounded by `CLAIM_BATCH`) and enqueues each as a fire-and-forget `CommitterRequest::RunScheduled`; the committer's `RunScheduled` arm (`committer.rs::handle_scheduled`) executes it via the normal `execute_txn` + `subs.fan_out` path and finalizes the row, so the single-writer invariant is intact. `when` is `afterMs`/`runAt` (one-shot); a past `runAt` fires immediately (catch-up). **At-least-once**: a crash between commit and finalize is recovered by `reset_running` on startup, so apps should write idempotent scheduled txns. Surface: WS `Schedule`/`CancelSchedule`/`PauseSchedule`/`ResumeSchedule`/`ListSchedules` and HTTP `POST /api/schedule`, `POST /api/schedule/{id}/{cancel,pause,resume}`, `POST /api/schedules`, with auth re-run on every WS op. Mirrored end-to-end: `ts-client`, `rust-client`, and `python-client` ship `schedule`/`cancelSchedule`/`pauseSchedule`/`resumeSchedule`/`listSchedules` (reactive WS + one-shot HTTP), and the TS in-memory test harness adds a timer-less `tick()`. |
| 10 | 2 | **Cron jobs** (recurring txns) | ✅ | ✅ | Med | S (after #9) | Implemented — recurrence rides the same `scheduled_txns` table and scheduler as #9: `when: {type:"cron", expr}` validates a 5-field standard Vixie-cron expression (**min-first**, UTC, via the `croner` crate) and sets the first `due_at`. After each successful fire, `handle_scheduled` recomputes the next fire (`scheduler::next_fire` → `finalize_cron_next`) and bumps `fired_count`; a failed cron fire reschedules to the next window and records `last_error` but keeps firing (unlike a failed one-shot, which goes `status='error'` and stops). Cron **skips** missed windows — no backfill: if the server is down across a fire time, that window is lost, not caught up. Same WS/HTTP/client surface as #9 (`cron` is just another `ScheduleWhen` variant). |
| 11 | 2 | **Full-text search** | ✅ search indexes | ✅ | Med | M | Implemented — a declared search index (`IndexDef` carries an additive `search: true` flag, omitted from the wire for ordinary btree indexes so existing schemas deserialize unchanged) compiles to a generated `tsvector` column (`to_tsvector('english', …)` over its text fields, coalesced for nulls) plus a GIN index on it; the `search` query terminal (`{index, query}` on `Query`, mutually exclusive with every other terminal) matches that tsvector against `plainto_tsquery($1)` and ranks by `ts_rank` DESC with `(created_at, id)` tie-breakers, composing with `take`. The query text is bound once via `$n` and reused in the `ORDER BY ts_rank`, so user text can never inject tsquery syntax; malformed search (unknown index, empty query) is a clear `BadRequest`. Mirrored end-to-end across all four clients — server, rust-client (`search_index()` schema declaration + `.search()` query builder), ts-client (`searchIndex()` + `.search()`), and python-client (`search_index()` + `.search()` DSL). Native Postgres fit: no external service, free where Convex needs a dedicated search-index component. |
| 12 | 2 | **Optimistic updates** | ✅ | ✅ | Med | M | Implemented (ts-client + rust-client) — opt-in `optimisticUpdates` (ts) / `Config.optimistic_updates` (rust, default off) on `RtDbClient` overlays a projected effect on each subscription's last result synchronously on `mutate`, then reconciles to the authoritative `queryUpdate` (server wins) and rolls back on `mutateErr`/reject/close. Correctness over coverage: only unambiguous projections (insert/patch/delete on known result docs) overlay; everything else waits for the server. Pure coverage in `optimistic.test.ts` (ts) and `optimistic.rs` unit tests (rust). |
| 13 | 1 | Extra validators: `record`, `int64`, `any`, `bytes` | ✅ | ✅ | Low–Med | S–M | Implemented — four new `FieldType` variants (`schema.rs`): `record` (dynamic string-keyed map, each entry validated against its `value` validator), `any` (accepts and stores any JSON value with zero validation), `bytes` (a JSON string validated as standard base64 with required padding, RFC 4648 §4), and `int64` (a JSON string of canonical decimal digits validated via `i64::from_str` — chosen because JSON numbers are IEEE-754 doubles and cannot exactly represent the full `i64` range past `Number.MAX_SAFE_INTEGER`). None of the other three get a DDL-indexed column — `record`/`any`/`bytes` aren't scalar-comparable; **`int64` is now indexable** (2026-07-28) as a `bigint` column, mirroring `Number`→`double precision`: eq/range/count/collect/unique/take/first/paginate/filter + aggregate `sum`/`avg`/`min`/`max` (server-only, no wire change), with `subs.rs::cmp_binds` carrying an `I64` arm so int64 range subscriptions invalidate correctly (without it the `None` fallback would skip re-runs — under-approximation). `SUM`/`AVG` over bigint return Postgres `numeric` → serialized as a JSON number (f64), so precision is lost past 2^53 (accepted). The ts- and rust-client in-memory harnesses thread an int64 storage type through their comparators so ordering/range/aggregate/paginate are numeric, not lexicographic. Mirrored end-to-end: `protocol.ts`'s `FieldTypeJson` and the client's `t.record()/t.any()/t.bytes()/t.int64()` factories, with schema/DDL/round-trip coverage in `schema_validators_test.rs` and factory/type coverage in `schema.test.ts`/`schema.types.test.ts`. **`int64` wire convention:** decimal-string on the wire, typed as a branded `Int64` string (not a real `bigint`) on the TS client — the client is entirely schema-type-erased at runtime (no codegen, no marshaling for any existing validator), so a real `bigint` would need a `JSON.stringify` replacer on writes and schema-aware result marshaling on reads that no other type needs; `Int64` instead follows the same zero-runtime-cost branded-string pattern already used for `Id<TableName>`, with `toInt64()`/`fromInt64()` helpers for apps that want actual `bigint` arithmetic. |
| 14 | 2 | **Additional OAuth providers** (Google, etc.) | ✅ many via integrations | 🟡 GitHub + Google + GitLab + generic OIDC | Med | M | Implemented — `OAuthProvider` trait (`auth/provider.rs`) with GitHub (refactored, routes byte-identical) and Google providers; each extra provider is now S. Identity is email-keyed with cross-provider same-email linking (both providers verified the email); Google requires a verified email. GitLab shipped (`server/src/auth/gitlab.rs`); generic OIDC shipped (`server/src/auth/oidc.rs` — any standards-compliant IdP: Azure AD, Keycloak, Auth0, Okta; configured via `RTDB_OIDC_*` endpoint URLs since the trait's sync `authorize_url` can't do live discovery). Remaining per-IdP impls (Microsoft, Apple) are each a small `provider.rs` impl. |
| 15 | 2 | **db-side `filter()`** expressions | ✅ (discouraged in favor of indexes) | ✅ | Med | M | Implemented (server + rust-client builder) — a tagged-enum predicate DSL (`eq`/`neq`/`gt`/`gte`/`lt`/`lte`/`in` + `and`/`or` combinators) compiled to a fully-parenthesized WHERE fragment with every identifier schema-validated + double-quoted and every value `$n`-bound; indexed fields use their typed column, others use jsonb extraction with a value-inferred cast. Composes with index/order/take/cursor/count; `get` rejects it; malformed → `BadRequest`, never a 500. Mirrored end-to-end across all four clients (server + rust-client + ts-client + python-client `.filter()` builders). Convex steers users to indexes, so this stays an opt-in terminal. |
| 16 | 3 | **File storage** | ✅ upload URLs, serving, metadata | ✅ | Med–High | L | Implemented — Postgres-native blobs (per the user's vendor-lock-to-Postgres steer: no disk/S3/object-store, no trait). A per-db `storage` table (`bytea`, TOAST-managed) holds each file's bytes + `{sha256, size, contentType, createdAt}`, and a global `rtdb.storage_index(id → db_name)` resolves the public serve URL to its owning database. Upload is `POST /api/storage/{db}` (bearer, raw body, `Content-Type`); the route disables axum's 2 MiB default and enforces `RTDB_MAX_FILE_SIZE` (default 50 MiB) via `axum::body::to_bytes`, rejecting overflow as `BadRequest`. Serve is both **public** `GET /storage/{id}` (no auth — anyone with the opaque uuid-v7 URL fetches it, Convex parity; revoke by delete) and **authed** `GET /api/storage/{db}/{id}` (caller-db-scoped). Plus `DELETE /api/storage/{db}/{id}` (idempotent, revokes the public URL) and `GET /api/storage/{db}/{id}/metadata`. Storage is **HTTP-only** (not reactive → no WS variants) and writes via `storage::put` directly, not the committer (blobs don't touch document tables or subscriptions). Mirrored on ts-client (`upload`/`deleteFile`/`getFileMetadata`/`getUrl`), rust-client (`upload`/`delete_file`/`get_file_metadata`/`get_url`), and python-client (`upload`/`delete_file`/`get_file_metadata`/`get_url`). |
| 17 | 3 | **Vector search** | ✅ | ✅ | Med | M–L | Implemented — the pgvector extension (`CREATE EXTENSION IF NOT EXISTS vector` in `db::create_database` and as the first statement in `ddl::push_schema`, so existing databases get it idempotently) backs a new `Vector { dimensions }` field type (`schema.rs`), stored in the `doc` jsonb as a JSON array and validated for exact length + finite entries (not btree-indexable). A vector index on `IndexDef` via additive `vector: Option<VectorIndexSpec { dimensions, filter_fields }>` (`skip_serializing_if`-omitted when absent, so existing btree/search indexes deserialize unchanged) compiles in `ddl.rs` to a write-maintained `vector(N)` column `v_<index>` (populated on insert/patch/replace, not a generated column — pgvector has no jsonb→vector generated cast) plus an HNSW `vector_cosine_ops` index; declared `filterFields` get their typed `f_` columns. The `vectorSearch` query terminal (`{index, vector, limit, filter?}`, mutually exclusive with every other terminal) ranks by cosine distance `<=>` ASC, carries its own `limit` (capped at `VECTOR_SEARCH_MAX_LIMIT = 256`), and takes an optional eq-`filter` over the index's declared `filterFields`; the query vector is bound via `$n::vector` text cast so user-supplied arrays can never inject syntax. It rides the committer's existing table-level invalidation, so subscriptions re-run and push on any write to the table — no new committer code. **Two deliberate divergences from Convex:** (1) **reactive** (Convex's `vectorSearch` is a one-shot action; par-rt-db re-runs and pushes live), and (2) **client-supplied embeddings** (no server-side generation — the architecture has no JS runtime). Mirrored end-to-end: server + ts-client (`t.vector(n)`, `vectorIndex(...)`, `.vectorSearch(...)`) + rust-client (`FieldType::vector(n)`, `vector_index(...)`, `.vector_search(...)`) + python-client (`t.vector(n)`/`vector_index(...)`/`.vector_search(...)`/`.hybrid_search(...)`, `VectorSearchQuery`/`HybridSearchQuery` wire types); the dev and prod Postgres image is `pgvector/pgvector:pg17` (vector 0.8.5, live and verified in both dev and prod as of 2026-07-25). |
| 18 | 3 | **Data browser dashboard** | ✅ full dashboard | ✅ backend (Phases 1–6) + frontend SPA | Med | L | In progress — expanded from a table browser into a full realtime dashboard (metrics, config, op feed). Backend built in phases; frontend via `/impeccable` after. **Phases 1–2 shipped**: Phase 1 — dashboard auth foundation (`AdminPrincipal` gate accepts the admin key **or** an OAuth session on a server-wide `rtdb_auth.admins` allowlist, `RTDB_ADMIN_EMAILS` seed, `auth::is_admin`, `GET/POST/DELETE /admin/admins`); Phase 2 — metadata read-back (`GET /admin/dbs/{db}/schema`, `GET /admin/tokens?db=` (no secret), `GET /admin/dbs/{db}/stats` (per-table row counts + storage sizes)). Full server suite green (362 tests). **Phases 3a/3b shipped**: live metrics (`GET /admin/metrics` — gauges/throughput) and a realtime op feed (`GET /admin/ops/recent`, `WS /admin/stream`). **Phase 4 shipped**: hot-reloadable config — `HotConfig` behind `Arc<ArcSwap<…>>` on `AppState`, persisted in a single-row `rtdb_config` table; `GET /admin/config` (redacted — `admin_key`/OAuth secrets/`database_url` → configured-bools) and `PATCH /admin/config` (subset patch, validated, persisted, swapped live); the CORS layer's `AllowOrigin::predicate` reads live `allowed_origins`, so an added origin takes effect with no restart. **Phase 5 shipped**: admin document access — `POST /admin/db/{db}/query|mutate` read/write with `owner=None` (bypass per-row `ownerField`); `/sync` admin bypass (`is_admin` at the handshake → skip the per-db `authorize` + `owner=None` on Subscribe/Mutate; machine tokens unaffected; `auth::authorize`/`owner_of` untouched); `RTDB_MAX_AFFECTED_DOCS` step-count cap (admin-only, default 100) rejects over-cap mutations before they reach the committer. 366 tests green. **Phase 6 shipped**: same-origin static SPA hosting — `ServeDir` (with an `index.html` SPA fallback) mounted as the router's last `fallback_service`, gated on `RTDB_STATIC_DIR` (unset/missing ⇒ API-only); a `Content-Type`-keyed `Cache-Control` middleware (text/html no-cache, assets immutable). **Backend complete** — 368 tests green, fmt+clippy clean, every phase Fable-reviewed. **Frontend shipped** — `/impeccable`-driven operator console SPA (Vite + React 19 + TS, a bun workspace linking `@par-rt-db/client` live): token-driven bespoke CSS in the committed "Instrument Manual" world (dark console, phosphor-green accent, mono data, hairline grids). Three-pane shell (command rail · main · live op-feed rail) with a topbar connection pulse. Surfaces: admin-key + OAuth (GitHub/Google) login; databases index + per-db stats; schema spec sheet; a **live data browser** (dense schema-driven doc table — true realtime over `/sync` for OAuth admins via the admin bypass, ~2s polling of `/admin/db/{db}/query` for admin-key mode; insert/patch/delete via `/admin/db/{db}/mutate` under the `RTDB_MAX_AFFECTED_DOCS` cap); a live metrics instrument panel; a full op-feed page; a hot-config editor (PATCH `/admin/config`, live reload) + read-only server spec; and the admin allowlist CRUD. The op feed + metrics stream live over a single WebSocket to `/admin/stream` — the admin bearer rides in the `Sec-WebSocket-Protocol` subprotocol (`rtdb-admin.<token>`) since browsers can't set the Authorization header on a WS handshake, and the server authenticates it at the upgrade (`admin.rs` → `bearer_from_subprotocol`/`authenticate_admin`). Gate green; verified live end-to-end. Spec: `docs/superpowers/specs/2026-07-24-realtime-dashboard-design.md`; plans: `docs/superpowers/plans/2026-07-24-realtime-dashboard-phase{1-auth,2-metadata,3a-metrics,3b-opfeed,4-config,5-admin-docs,6-static}.md`. |
| 19 | 3 | **Client test harness** (in-memory fake) | ✅ `convex-test` | ✅ | Med | M | Implemented (ts-client + rust-client) — `InMemoryRtDbClient` mirrors the server's schema/query/txn/step-result semantics with no network and no Postgres: `pushSchema`, `query`/`run`/`run_query`, `mutate` (with `mutId` idempotency), `subscribe` (reactive — re-runs and fires on a real change), cursor-keyset `paginate`, system fields merged at read time, atomic rollback on step failure. Both ports reuse their crate's wire types (`protocol.ts` / `wire.rs`). `filter()` expressions are evaluated against in-memory rows (server-parity via `validateFilter`/`evalFilterExpr`); `search`/`vectorSearch` return `[]` as honest stubs (no in-memory ranking — rejected combinations still throw). **ts-client** ships it unconditionally (`src/in_memory.ts`, coverage in `tests/in_memory.test.ts`). **rust-client** ships it behind the `in_memory` feature (`src/in_memory.rs`) — a direct port adding the schedule harness (`schedule`/`cancel_schedule`/`pause_schedule`/`resume_schedule`/`list_schedules` + a timer-less `tick(now_ms)` that mirrors the server's catch-up semantics: one-shot fires once even when past `due_at`; cron steps by `CRON_STEP_MS = 60_000` and skips missed windows) and the storage stubs (`upload`/`delete_file`/`get_file_metadata`/`get_url`) so app-level flows are exercisable with no network. The `in_memory` feature gates an optional `sha2` dep for upload digests; coverage in `in_memory.rs::tests`. **Additive schema evolution shipped** (2026-07-28): both harnesses now mirror the server's additive-only `pushSchema`/`push_schema` — porting `ddl.rs::detect_destructive_changes` (reject removed/changed tables, fields, and indexes with the server's `BAD_REQUEST` messages) and merging additively (preserving existing docs + the idempotency cache on a second push, seeding only new tables, never wiping). **Literal-union widening now mirrored** across all three harness ports — `isWideningOf` (ts-client), `is_widening_of` (rust-client), and `_is_widening_of` (python-client) port `schema::is_widening_of`, so a safe widening of a literal-union field (e.g. `{a,b}`→`{a,b,c}`, or `"a"`→`{a,b}`) is accepted on a second `pushSchema` instead of rejected as a type change. |
| 20 | 3 | **Per-row authorization rules** | ✅ arbitrary code in functions | ✅ owner + collaborator match | Med–High | XL | Implemented (v1: owner-field match; v2: + collaborator field) — a table opts in by declaring `ownerField` (names a declared, string-compatible field holding the owner's `user_id`); the declaration is additive/optional, so existing schemas deserialize unchanged, and it mirrors byte-identically across all four schema representations (server `TableDef.owner_field`, ts-client `defineTable(...).ownerField(...)`, rust-client `TableBuilder::owner_field(...)`, python-client `TableBuilder.owner_field(...)`) — wire key `ownerField`, `skip_serializing_if`-omitted when unset. Server-enforced and unforgeable on every terminal: `index`/`take`/`collect`/`count`/`paginate`/`unique`/`first` get an injected `FilterExpr::Eq` AND-ed with the client filter, `get(id)` returns a silent `Doc(None)` for unowned rows, and `search`/`vectorSearch` carry an owner predicate in their SQL; the owner value is `$n`-bound, never interpolated. Subscriptions capture the subscriber's owner at subscribe time and re-filter to it on every `fan_out`, so a write by user B never pushes into A's subscription. Writes: `insert` and `upsert`-insert auto-stamp the owner server-authoritatively (overwriting any client value), and `patch`/`replace`/`delete`/`upsert`-update run an ownership pre-check INSIDE the serialized transaction — mismatch returns `Forbidden` (HTTP 403) and aborts the whole txn atomically; `ownerField` is immutable post-insert (re-stamped on every write, so a user cannot transfer or orphan ownership). Machine tokens and scheduled jobs (no interactive principal) bypass per-row rules entirely — the db-level allowlist/token/session gate still runs first, and per-row is an additive second layer. Model B (2026-07-28) adds an optional `collaboratorsField` alongside `ownerField`: it names a declared array-of-strings (or array-of-id) field, validated as such at push time, and access widens to owner OR collaborator — reads AND-in `owner = uid OR uid = ANY(collaborators)`, the write pre-check accepts either, and subscriptions inherit the same predicate through `fan_out`. A table may declare either field or both, and declaring only `collaboratorsField` still enables per-row enforcement; unset behaves byte-identically to owner-only. Mirrored across all four clients (wire key `collaboratorsField`; `defineTable(...).collaboratorsField(...)`, `TableBuilder::collaborators_field(...)`, `TableBuilder.collaborators_field(...)`). Unlike the owner, the collaborators array is an ordinary document field — any principal who may write the row may edit it. **Model C shipped (2026-08-02):** a third opt-in `authorize` declaration — a `FilterExpr` predicate over doc fields plus `$user`/`$email` principal markers — generalizes owner/collaborator to any declarable rule (tenant scoping, `visibility == "public" OR owner`, `editors[] ∋ caller AND archivedAt IS NULL`), enforced on reads (silent filter), writes (pre-check + auto-stamp of `$user` fields + post-write verify → `Forbidden`/403, atomic, on all five write paths for `ownerField` parity), and subscription re-runs; the `FilterExpr` gains `Not`/`Contains`/`Exists` (also usable in client `.filter()`), principal markers are valid only in `authorize`; mirrored across all four clients (schema `authorize` + new `FilterExpr` variants). Design: `docs/superpowers/specs/2026-08-02-per-row-auth-predicate-dsl-design.md`. Still deferred: `ExpectVersion`/`ExpectAbsent` (an existence/version side-channel, not owner-checked). |
| 21 | 3 | **Fine-grained subscription invalidation** | ✅ read-set tracking | ✅ point + eq-prefix/range + ordered top-N | Low (now) | L | v1: `get(id)` point reads skip re-runs when the write didn't touch their document (sound — a point read depends on exactly one doc). v2 (2026-07-28): `count` / `collect` / `unique` filtered on a btree index's eq-prefix (+ optional range bound on the next field) skip re-runs when every written doc is provably outside the window — `WriteSet.doc_values` carries each written doc's before/after state, `ReadSet::Indexed` evaluates `Window::contains` per written doc (deleted⇒re-run; `count` is membership-only, `collect`/`unique` are content-bearing; any typing/missing-field doubt ⇒ over-approximate to re-run, so it never under-approximates). v3 (2026-07-29): `take(N)` / `first` / `paginate` — the ORDERED, truncated shapes — skip too. `ReadSet::Ordered` pairs the same window with the sort key of the last computed result's final row (the boundary, mirroring `execute_query`'s `ORDER BY <index fields beyond the eq-prefix>, created_at, id`), and re-runs only when a written doc is inside the window AND ranks at or before it in either its before- or after-state: a doc beyond the boundary cannot be in the top N, and cannot displace a member either (displacement requires a member to leave, whose own write already triggers the re-run). The boundary is seeded from the initial result and refreshed from every re-run; an unfull result (fewer than N docs, or a page with no next cursor — an insert past a full-but-last page would flip `hasNext`) leaves it unset, degenerating to plain membership. Ranking needs each written doc's `created_at`, which `WriteSet.doc_values` now carries (a `Delete` captures none ⇒ deletes always re-run). Doubt always re-runs: unrankable docs (missing/null/wrongly-typed sort field), exact ties with the boundary (the DB breaks those on `id` under its own collation, deliberately not modeled), and unresolvable indexes fall back to `Table`. `distinct` / `aggregate` / `search` / `vector` / `hybrid` stay table-level — their results depend on member VALUES or a ranking function. Server-only, no protocol change; `DocOp` unchanged so op-feed/audit/webhook stay byte-identical. Design + soundness arguments: `docs/superpowers/specs/2026-07-24-fine-grained-subscription-invalidation-design.md`. |
| 22 | 2 | **Unique + partial (`WHERE`) index constraints** | ❌ (no first-class declarative unique constraint) | ✅ | High | M | Implemented — two additive `skip_serializing_if`-omitted flags on `IndexDef` — `unique: bool` (wire key `unique`, omitted when false) and `where: Option<FilterExpr>` (wire key `where`, omitted when `None`) — so existing schemas deserialize unchanged. The `where` reuses the query-time `filter()` `FilterExpr` DSL, but compiled to **literal** SQL at DDL time (`compile_filter_literal` — Postgres partial-index predicates forbid bind params). The btree DDL branch emits `CREATE [UNIQUE] INDEX … [WHERE <literal>]`; a dup pre-check gives a friendly error before CREATE, with `CREATE UNIQUE INDEX` itself the authoritative race-free guarantee. A UNIQUE index covers its declared `fields` only — no `created_at` tiebreaker (a per-row-distinct column would defeat uniqueness); a partial unique index constrains only rows matching its `where` predicate ("unique slug among non-deleted rows"). Postgres enforces uniqueness inside `execute_txn`; a `unique_violation` (SQLSTATE 23505) maps to a dedicated **`CONFLICT` (HTTP 409) wire code** (added to all four clients). `unique`/`where` are legal only on a plain btree index (rejected alongside `search`/`vector`); changing either on an existing index is destructive (rejected on push — use migrate `dropIndex` + re-push). Mirrored end-to-end across server + ts-client + rust-client + python-client, including in-memory test-harness enforcement. Postgres-native: free where Convex would need application-level checks — par-rt-db leads here. |
| 23 | 2 | **Document TTL / auto-expiry** | ❌ (apps self-roll via scheduled functions/cron) | ✅ | High | M | Implemented — a table declares `ttl: { field, defaultDurationMs? }` (additive, `skip_serializing_if`-omitted) naming a declared numeric field (`number`/`int64`) whose value is each document's absolute epoch-ms expiry; `defaultDurationMs` stamps it at insert when the client omits it. Validation requires a single-field, non-unique, non-partial btree index on `ttl.field` (so the reaper's range scan is indexed). A per-db **reaper** task (`reaper.rs`, spawned in `Committers::channel_for` alongside the scheduler + mutation-log cleanup, same self-termination lifecycle) periodically enqueues a fire-and-forget `CommitterRequest::RunReaper` every `RTDB_TTL_SWEEP_INTERVAL_SECS`; the committer's `RunReaper` arm (`handle_reaper`) batch-deletes expired rows (`DELETE … WHERE f_<field> IS NOT NULL AND f_<field> < now() LIMIT N`) inside the committer's serialized turn and publishes through all four tap sites (fan-out / op-feed / audit / webhook) with `source = "ttl"`, `owner = None` — so a TTL delete fires subscriptions, op-feed, audit, and webhooks like any user delete (a delete captures no `doc_values` ⇒ table-level fan-out re-run). TTL deletes bypass per-row `ownerField` (system-initiated, like scheduled jobs). Adding `ttl` with a default backfills existing rows to `created_at + default` (both the jsonb `doc` and the typed column, since reads return the doc). Boot config `RTDB_TTL_SWEEP_INTERVAL_SECS` (60) / `RTDB_TTL_BATCH` (5000); metric `rtdb_ttl_expired_total`. Mirrored end-to-end across server + ts/rust/python clients (schema `ttl` + in-memory `tick()` reaping folded into the harnesses' existing scheduled-job time-advance). Mongo-style (field + `defaultDurationMs`, like a Mongo TTL index) and first-class where Convex has none — par-rt-db leads here. |

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

## 4. Dimensional comparison

The parity table (§1) marks features present in both. This section compares the two systems
across the dimensions a feature checklist hides — what each one *is*, not just what it *has*.
Convex facts are from its current docs (sources at the end).

### Data model & storage

**Convex** stores documents in its own storage engine (Postgres/SQLite/MySQL on self-host),
modeled by `schema.ts` validators, with separate subsystems for text search (`searchIndex`),
vectors (`vectorSearch`, via a component), and files (a storage API).

**par-rt-db** is one Postgres 17 database per app. Documents are a `doc` jsonb column with
system fields merged at read time; each indexed field gets a real typed, btree-indexed Postgres
column (index reads are index scans, not scan-then-filter). Text search is a generated
`tsvector` + GIN, vectors are a pgvector write-maintained column + HNSW, and files are `bytea`
blobs in a per-db storage table. Postgres is the only datastore — by design (user-approved
vendor lock).

**Tradeoff:** par-rt-db gets search/vector/count/files "for free" from Postgres where Convex
wires components, and the `psql` escape hatch is always open. Convex offers engine choice on
self-host and a more uniform managed experience in the cloud.

### Consistency & transactions

**Convex** is ACID with **serializable** isolation via **optimistic concurrency control**:
transactions run without locks, are validated at commit, and are **automatically retried** on
conflict. Every mutation function is a fully serializable transaction ([How Convex works][hcw],
[Overview][ov], [OCC & atomicity][occ]).

**par-rt-db** **serializes all writes through a single per-database committer task** (true
serial execution — one mutation at a time, in arrival order), while reads run at **READ COMMITTED
with no row locking**. There is no OCC retry: a mutation commits in its turn or is rejected (a
failed `expectVersion`/`expectAbsent` aborts the whole txn). Retry is explicit and opt-in via an
idempotency key (#4), never automatic. That same serialization is what makes live-query
invalidation sound — the committer re-runs affected subscriptions between writes.

**Tradeoff:** Convex offers the strictest stated guarantee and lets independent transactions run
concurrently (auto-retry under contention); par-rt-db trades concurrency for simplicity and
predictability — serial writes, zero surprise retries, but write throughput bounded by one
serialized path per database (a non-goal to scale out — §3). Reads are READ COMMITTED snapshots,
not serializable: a deliberate, documented choice.

### Scaling & availability

**Convex** (cloud) is horizontally scaled, multi-tenant, with managed replication and backups.
**par-rt-db** is single-writer per database on one server instance hosting many databases, backed
by one Postgres. Multi-region / horizontal scale-out is a deliberate non-goal (§3) — the
single-writer committer is load-bearing for correctness. Availability is whatever your single
Postgres + host provide (nightly `pg_dump`; single host today).

**Tradeoff:** Convex scales for you; par-rt-db is intentionally single-node, tuned for
personal/agent-scale apps where one box is plenty.

### Deployment & operations

**Convex** is `npx convex dev` / `npx convex deploy` to the cloud (zero-ops), or [self-host the
open-source backend][sh] via Docker Compose (Postgres/SQLite/MySQL). Self-host runs the V8
function runtime + storage engine + dashboard; you still host your frontend separately.

**par-rt-db** is one Rust binary (axum/tokio) + one Postgres 17, deployed via plain
`docker compose` behind a Cloudflare tunnel; the operator dashboard is a same-origin SPA baked
into the image. No function runtime, no separate search/vector/file services — Postgres carries
all of them. A new app is an admin call to create a database, not a new deployment.

**Tradeoff:** par-rt-db is a smaller, Postgres-native stack with fewer moving parts and one
datastore to back up; Convex gives a richer managed cloud if you want zero ops, and an
open-source self-host if you don't (but it's a bigger system to run).

### Cost model

**Convex Cloud** is usage-based: free tier (1M function calls/mo, 0.5 GB storage), then
pay-as-you-go (~$2.20/M function calls, ~$0.22/GB-month storage), per-project Professional +
usage, custom Enterprise ([pricing][pr]). **par-rt-db** is self-hosted on your own hardware and
Postgres — zero per-call, per-seat, or per-GB metering; cost is the box + Postgres you'd run
anyway.

**Tradeoff:** Convex Cloud bills on usage (cheap on the free tier, grows with scale); par-rt-db
is a fixed-cost owned asset. Convex's open-source self-host removes the metering too — so the
real differentiator is operational shape (single binary over Postgres vs a runtime+engine stack),
not "SaaS vs owned."

### Developer workflow

**Convex** has you write TypeScript server functions (queries/mutations/actions); `npx convex
dev` watches and **codegens** a typed API + data model into `_generated/`. Functions are the API;
you deploy functions; a rich component ecosystem extends them.

**par-rt-db** has **no codegen** — the schema object is the source of inferred types (`Doc`,
`Id`), and there are **no server functions at all**: clients send a declarative JSON DSL (typed
queries + atomic multi-step transactions). "Deploying" is pushing a schema; behavior lives in the
client. Anything the DSL can't express goes to an external worker with a machine token (§3).

**Tradeoff:** Convex's function model is more expressive (arbitrary server logic, side-effect
actions, HTTP endpoints) but heavier (runtime, codegen, deploy cycle); par-rt-db is a thinner
loop — schema push + client DSL, types inferred, no server deploys.

### Security & auth model

**Convex** does function-level auth — you check `ctx.auth` inside each function (Convex Auth,
Clerk, Auth0, or custom JWT). **par-rt-db** authenticates per database (machine tokens + OAuth
sessions + email allowlist) and authorizes **centrally**, re-running `authorize` on every
Subscribe/Mutate over an open WS so revocation/allowlist/expiry take effect live — plus opt-in
per-row `ownerField`/`collaboratorsField` enforced on every read, every `fan_out`, every write
(#20).

**Tradeoff:** Convex gives arbitrary auth logic in code (flexible, but you must apply it
consistently everywhere); par-rt-db centralizes auth and per-row rules at the protocol layer so
they can't be bypassed by a forgotten check.

**OAuth popup hardening (SEC-012):** the OAuth popup opens with `noopener,noreferrer` (reverse-tabnabbing defense), and login completion is relayed by the parent polling `GET /auth/state?state=<token>` keyed on the single-use, TTL-bounded state token — not by `window.opener.postMessage` (which `noopener` severs). `GET /auth/{provider}/begin` mints the state and returns the provider authorize URL; the callback sets the HttpOnly session cookie and closes the popup without interpolating the parent origin (the prior self-XSS surface is retired). The state token (not the cookie) is the poll capability, so the flow works cross-origin (e.g. an app on a different origin than the server) where the `SameSite=Lax` session cookie would not be sent. `noopener` trades away blocked-popup detection and a closed-popup signal; the poll's deadline covers both.

### Observability

**Convex** offers a managed dashboard (logs, function traces, metrics), log streams/integrations,
and streaming export. **par-rt-db** exposes Prometheus text on `/metrics` + JSON on
`/admin/metrics` (throughput, p50/p95/p99, pool/WS/subscription gauges, `rtdb_build_info`), a
realtime op feed + audit log + webhooks, self-checking invalidation counters
(`rtdb_subs_skips_total` / `reruns` / `missed_pushes`, #21), and an operator dashboard.

**Tradeoff:** Convex's cloud observability is richer and managed; par-rt-db gives standard
Prometheus + an operator console you scrape yourself — fewer integrations out of the box, full
raw access to your own Postgres.

[hcw]: https://stack.convex.dev/how-convex-works
[ov]: https://docs.convex.dev/understanding/overview
[occ]: https://docs.convex.dev/database/advanced/occ
[pr]: https://www.convex.dev/pricing
[sh]: https://docs.convex.dev/self-hosting

## 5. Same feature, different model

Both systems have the rows in §1 — but "implemented" hides how differently each one works. This
table surfaces the substantive model differences behind the ✅/✅, with the source row and which
way it cuts (⬆ par-rt-db · ⬆ Convex · ⚖ tradeoff).

| Source | Capability | Convex model | par-rt-db model | Edge |
|---|---|---|---|---|
| §1 (system fields) | Doc versioning | internal `_version`, not surfaced | `_version` on every doc → client-side OCC via `expectVersion` | ⬆ par-rt-db |
| #4 | Mutation retry | automatic, exactly-once | explicit, opt-in idempotency key (5-min); default at-most-once | ⚖ |
| §1, §4 | Read/write consistency | serializable + OCC + auto-retry | serial-writer committer + READ COMMITTED, no retries | ⚖ |
| §1 (schema migration) | Schema changes | additive + migrate/backfill | additive on push; destructive/type-changing via declarative migrate directives (`renameField`/`renameTable`/`changeType`/`drop*`/`setDefault` + scoped `evalExpr`) | ⚖ |
| #3, #3b | `count` / `distinct` | needs aggregate component / no native | native `SELECT COUNT(*)` / `SELECT DISTINCT` | ⬆ par-rt-db |
| #11 | Full-text search | search index (built-in) | generated `tsvector` + GIN | ⚖ |
| #17 | Vector search | one-shot action; embeddings via component/client | reactive, pushes live; client-supplied (no runtime) | ⚖ |
| #15 | db-side filter | discouraged; steer to indexes | indexes **and** a `filter()` predicate DSL | ⬆ par-rt-db |
| §1, #20 | Authorization | per-function `ctx.auth` checks | central `authorize` every op + per-row `ownerField`/`collaboratorsField` | ⚖ |
| §1, #18 | Control plane | managed dashboard + CLI | self-hosted operator console + HTTP admin + `rtdb` CLI | ⚖ |
| #16 | File storage | upload URLs + storage API; object store | per-db Postgres `bytea`; public opaque-UUID URL + authed serve | ⚖ |
| §1, §4 | Types & deploy | TS server functions + codegen (`_generated/`) | declarative DSL, no codegen, types inferred from schema | ⚖ |
| §3 | Server-side logic | actions / HTTP actions (V8) | none — external worker + machine token | ⬆ Convex |

**Where par-rt-db pulls ahead.** Postgres makes the terminals Convex builds components for —
`count`, `distinct`, full-text search, vector search, file storage — native and free; `_version`
is exposed for client-side OCC; there's no codegen/deploy loop; and auth + per-row rules live at
the protocol layer, so they can't be bypassed by a missed check.

**Where Convex pulls ahead.** Arbitrary server-side logic (actions, custom HTTP endpoints), the
strictest consistency guarantee (serializable), and managed horizontal scale — all consequences of
running user code on managed infra, which par-rt-db deliberately doesn't do (§3).

**Genuine tradeoffs.** Retry semantics (auto vs explicit), the read guarantee (serializable vs
READ COMMITTED snapshots), and schema-change safety (backfill vs additive-only) are different
operating points, not better-or-worse — a team should pick them deliberately.

## 6. Where par-rt-db is ahead

- **No codegen** — schema *is* the TS source of types; no `npx convex dev` watcher, no
  generated `_generated/` directory to keep in sync.
- **Postgres-native and owned** — data in a Postgres you can `psql` into; nightly `pg_dump`
  backups. (Convex is now self-hostable too via its open-source backend, but runs a V8 function
  runtime + its own storage engine + dashboard; par-rt-db is a single Rust binary over one
  Postgres, with no per-call / per-seat / per-GB metering.)
- **SQL escape hatch** — anything the DSL can't express yet has a manual answer today.
- **`_version` on every doc** — explicit optimistic-concurrency primitive
  (`expectVersion`) that Convex doesn't surface.
- **Explicit at-most-once mutations** — no hidden auto-retry semantics (#4 adds
  opt-in safe retry without giving this up).
- **One instance, many databases** — a new app is an admin call, not a new deployment.
- **Declarative unique + partial indexes** — a schema flag compiles to
  `CREATE [UNIQUE] INDEX … [WHERE]`, with Postgres enforcing uniqueness and a dedicated
  `CONFLICT` (409) error code. Convex has no first-class declarative unique constraint
  (#22).

## 7. Status

As of 2026-08-01, **all ranked gaps are shipped** — every row in §2 is ✅ (the original
21 plus #22 unique/partial indexes, a par-rt-db advantage Convex lacks), including the
#18 dashboard (backend + frontend). The per-row Notes are authoritative; the shape of
the larger builds:

- **Scheduling (#9/#10)** — one per-db `scheduled_txns` side table drained through the
  committer by a per-db scheduler timer (`scheduler.rs` + the `RunScheduled` arm):
  at-least-once, one-shot catches up if past due, cron skips missed windows.
- **Storage (#16)** — per-db `bytea` `storage` table + global `storage_index`; public
  (`GET /storage/{id}`) and authed serve, upload/delete/metadata; HTTP-only,
  Postgres-native.
- **Vector search (#17)** — pgvector-backed: `Vector` field type + write-maintained
  `vector(N)` column + HNSW `vector_cosine_ops` + reactive `vectorSearch` terminal
  (cosine `<=>`, eq-`filter` over `filterFields`, limit ≤256; client-supplied embeddings,
  no server-side generation). **Live in dev and prod** (`pgvector/pgvector:pg17`,
  vector 0.8.5 — verified 2026-07-25).
- **Per-row auth (#20)** — opt-in `ownerField`, enforced on every read terminal, every
  `fan_out` (subscriber's owner captured at subscribe time), and every write (auto-stamp
  on insert + `upsert`-insert; ownership pre-check inside the serialized txn on
  `patch`/`replace`/`delete`/`upsert`-update → `Forbidden`/403; owner immutable
  post-insert). Model B (`collaboratorsField` — owner OR collaborator) and model C
  (`authorize` — a general `FilterExpr` predicate over doc fields + `$user`/`$email`
  principal markers; pre-check + auto-stamp `$user` fields + post-write verify on all
  five write paths, `ownerField` parity) have shipped, mirrored across all four clients.
  `ExpectVersion`/`ExpectAbsent` (an existence/version side-channel, not owner-checked)
  remains deferred (see the row).

The four clients share one wire contract across db-side `filter()`, full-text search,
and vector search: server (`protocol.rs`), ts-client, rust-client, and python-client.
The python-client ships the wire contract, the schema/mutation/query DSL, **the
HTTP/admin/storage surfaces** (`pip install par-rt-db[http]` — a sync `httpx` client; admin
client + storage helpers; schema `FieldType(15)`/indexes/`ownerField`, `TableQuery`
builders, `Mutation`/`Transaction`, `FilterExpr`, `RtDbError`), **and the reactive WS
surface** (`pip install par-rt-db[ws]` — `RtDbClient` with live `subscribe`, at-most-once
`mutate`, and schedule ops over `/sync`; `Subscription` async iterator with `.current()`).
Reactive WS / live queries / WS mutations / schedule ops are mirrored across all four
clients: ✅ts ✅rust ✅python; the HTTP one-shot schedule ops (`POST /api/schedule*`) and
`/api/query-batch` are in the python HTTP client too. **The four clients are now at feature
parity** (2026-07-29): python ships optimistic updates (port from rust), an in-memory/offline
test harness (`par_rt_db.in_memory` + `tick()`), and the full admin control plane
(allowlist/admins/metrics/hot-config/ops-feed/tokens/schema/stats); ts-client `mutate()`
returns a typed `StepResult` with an `idempotencyKey` option (`mutId` deprecated); rust-client
`vector_search`/`hybrid_search` take opts structs. All four clients' in-memory test
harnesses evaluate the `distinct`/`aggregate` terminals (the live server is the
source of truth for both). Cross-language API-ergonomic differences (keyword args, casing) are intentional, not gaps.
