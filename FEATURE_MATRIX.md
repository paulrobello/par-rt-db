# Feature Matrix — Convex vs par-rt-db

**Date:** 2026-07-21
**Purpose:** Inventory Convex's feature surface against par-rt-db's, and rank every gap
by utility and level of effort so parity work can be picked off in value order.
**Perspective:** "Utility" is judged for the apps this instance actually serves (kanban
board, personal SPAs, CLI/agent tooling) — not for a hypothetical SaaS at scale.
**Sources:** `docs/superpowers/specs/2026-07-21-par-rt-db-design.md`, the implemented
server (`server/src/`), the client SDK (`client/src/`), and Convex's documented feature
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
| User auth | ✅ Clerk/Auth0/custom JWT/Convex Auth | 🟡 GitHub OAuth + sessions | One provider; per-database email allowlist replaces per-function auth checks |
| Live permission revocation | ✅ | 🟡 | `authorize` re-runs on every Subscribe/Mutate; session *expiry* mid-connection is a known gap (row 8 below) |
| Multi-app hosting | ✅ projects/deployments | ✅ named databases | One instance, many DBs — lighter than Convex's per-deployment model |
| Typed error envelope | ✅ | ✅ | `{code, message}`, seven codes, both transports |
| Schema migration on push | ✅ | 🟡 additive-only | Destructive pushes rejected by design; Convex allows more (with backfill) |
| Admin control plane | ✅ dashboard + CLI | ✅ HTTP + `admin.ts` | create-db, push-schema, tokens, allowlist |

## 2. Gap matrix — ranked by utility ÷ effort

Rank 1 is the best next investment. Tier 1 = quick wins, Tier 2 = medium builds with
high leverage, Tier 3 = large projects.

| # | Tier | Feature | Convex | par-rt-db | Utility | Effort | Implementation sketch |
|---|---|---|---|---|---|---|---|
| 1 | 1 | Index **range queries** (`gt/gte/lt/lte` after the `eq` prefix) | ✅ | ✅ | High | M | Implemented — `Query` carries optional `gt`/`gte`/`lt`/`lte` bounds on the index field immediately after the `eq` prefix, typed via the existing `eq_binds`/`eq_bind_for` conversion in `txn.rs` (no forked typing). Mirrored end-to-end: `protocol.rs`/`protocol.ts` wire shape and `TableQuery.gt()/.gte()/.lt()/.lte()` in the TS client, with integration coverage in `query_test.rs` and `query.test.ts`. |
| 2 | 1 | **`.first()`** terminal | ✅ | ✅ | Med | S | Implemented — sugar over `take(1)`: `first: bool` on `Query`, mutually exclusive with `take`/`unique` (and with `get`, like the other terminals), returns `Doc(Some)`/`Doc(None)` instead of `Docs`. Mirrored end-to-end: `protocol.ts` wire shape and `TableQuery.first()` in the TS client, with integration coverage in `query_test.rs` and builder-shape coverage in `query.test.ts`. |
| 3 | 1 | **`count()`** terminal | 🟡 (needs aggregate component) | ✅ | Med | S | Implemented — `count: bool` on `Query`, a terminal running `SELECT COUNT(*)` over the same eq-prefix + range-bound WHERE clause every other terminal builds, uncapped by the 4096-row take limit; mutually exclusive with `get`/`take`/`unique`/`first`/`order` the same way `first` is. `QueryResult` gains an untagged `Count(i64)` variant, so it flows as a plain JSON number over both one-shot HTTP and the reactive WS query with no special-casing. Mirrored end-to-end: `protocol.ts` wire shape and `TableQuery.count()` in the TS client, with integration coverage in `query_test.rs` and builder-shape coverage in `query.test.ts`. Postgres makes this free where Convex needs a sharded-counter component — we exceed Convex here. |
| 4 | 2 | **Safe mutation retry** (idempotency keys) | ✅ auto-retry, exactly-once | ❌ at-most-once by design | High | M | Server-side dedup table keyed by `mutId` (result cached, TTL'd); client can then retry on reconnect without double-apply. Biggest reliability gap for flaky networks; today only the explicit `PRECONDITION_FAILED` helper retries. |
| 5 | 2 | **Pagination** (cursor + `usePaginatedQuery`) | ✅ | ❌ | High | M–L | Opaque cursor = last row's index-key tuple; `paginate {cursor, numItems}` terminal is M. The reactive `usePaginatedQuery` page-stitching in `react.tsx` is the hard half. Prerequisite: row 1's range support for keyset pagination. |
| 6 | 1 | **`replace`** step (full-document overwrite) | ✅ | ❌ | Med | S | Like `patch` but validates the complete doc and rewrites all indexed columns. Straightforward `Step` variant in `txn.rs`. |
| 7 | 1 | **Snapshot export / import** per database | ✅ | ❌ | Med | S–M | `/admin/export-db` streaming JSONL (+ schema), `/admin/import-db` inverse. Complements host-level `pg_dump` with app-level portability (seed data, clone-to-dev). |
| 8 | 2 | **Live session-expiry enforcement** on open WS | ✅ | 🟡 | Med | S | Already on the kanban backlog. `authorize` re-runs per op but doesn't check session expiry; add the expiry check to that path. |
| 9 | 2 | **Scheduled transactions** (`runAfter`/`runAt` analog) | ✅ schedules functions | ❌ | Med–High | M | Because par-rt-db txns are *data*, not code, scheduling is a table of `(due_at, txn)` drained through the committer by a timer task — no JS runtime needed. Covers the main thing actions+scheduler give Convex apps (deferred writes, TTL cleanup). |
| 10 | 2 | **Cron jobs** (recurring txns) | ✅ | ❌ | Med | S (after #9) | Recurrence rule on the same scheduled-txn table. |
| 11 | 2 | **Full-text search** | ✅ search indexes | ❌ | Med | M | Postgres `tsvector` column + GIN index per declared search index; `search` query terminal with relevance ordering. Native fit — no external service. |
| 12 | 2 | **Optimistic updates** | ✅ | ❌ | Med | M | Client-only: overlay local writes on the last pushed result per subscription, reconcile on next `queryUpdate`. `client.ts` already tracks last results, so the seam exists. |
| 13 | 1 | Extra validators: `record`, `int64`, `any`, `bytes` | ✅ | ❌ | Low–Med | S–M | `record` (dynamic string-keyed maps) is the one apps actually hit; jsonb storage needs no DDL change. `int64` needs a bigint wire convention; `bytes` base64. |
| 14 | 2 | **Additional OAuth providers** (Google, etc.) | ✅ many via integrations | ❌ GitHub only | Med | M | Generalize `auth/github.rs` into a provider trait; each extra provider is then S. |
| 15 | 2 | **db-side `filter()`** expressions | ✅ (discouraged in favor of indexes) | ❌ | Med | M | Small predicate DSL compiled to SQL over `doc` jsonb / typed columns. Convex itself steers users to indexes, so ranked below range queries deliberately. |
| 16 | 3 | **File storage** | ✅ upload URLs, serving, metadata | ❌ | Med–High | L | Per-db blob table (or disk/S3 backend), tokened upload endpoint, public serve URL, `_storage`-style metadata, GC for orphans. Needed the moment any app wants image upload. |
| 17 | 3 | **Vector search** | ✅ | ❌ | Med | M–L | `pgvector` extension + vector field type + index + `vectorSearch` terminal. Cheap infra-wise; ranked lower because current apps don't need it yet. |
| 18 | 3 | **Data browser dashboard** | ✅ full dashboard | ❌ | Med | L | Read/write table browser SPA over the admin API (could itself run on par-rt-db). Convex's dashboard is a real DX advantage; `psql` is the stopgap. |
| 19 | 3 | **Client test harness** (in-memory fake) | ✅ `convex-test` | ❌ | Med | M | In-memory `RtDbClient` implementing the schema/query/txn semantics for app unit tests; today apps need a live server (integration tests only). |
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
- **Explicit at-most-once mutations** — no hidden auto-retry semantics (#4 would add
  opt-in safe retry without giving this up).
- **One instance, many databases** — a new app is an admin call, not a new deployment.

## 5. Recommended order

Work the gap matrix top-down; the natural first batch is **#1–#3 + #6** (range queries,
`first`, `count`, `replace`) — one cohesive "query/txn surface parity" unit touching
`query.rs`/`txn.rs`/`protocol.ts`/`query.ts`/`mutation.ts`, each individually small and
testable in the existing integration binaries. Then **#4 (safe retry)** and **#5
(pagination)** as the two medium builds with the highest app-facing payoff, with **#8**
folded in whenever auth code is next open. **#9/#10 (scheduled/cron txns)** is the
sleeper: it delivers most of what apps use Convex's scheduler+actions for, without
compromising the no-server-code architecture.
