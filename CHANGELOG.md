# Changelog

All notable changes to par-rt-db will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Each package is versioned independently; the four client SDKs are at `0.1.0` until
the first tagged release.

Feature entries cross-reference the rows in
[`FEATURE_MATRIX.md`](FEATURE_MATRIX.md), which is the authoritative parity
contract against Convex.

## [Unreleased]

### Audit remediation (2026-07-25)

Comprehensive remediation of the 2026-07-25 project audit (55 findings; 46
resolved, 9 deferred/no-action per the audit's own verdicts). The full
`make checkall` gate is green. Highlights:

- **Security**: dashboard credentials (admin key + OAuth session token) moved
  into an HttpOnly `rtdb_session` cookie so no secret is ever held in JS
  (SEC-001, both phases — `Auth.token` became optional across all four clients
  so `/sync` can authenticate from the cookie); `is_admin` re-run per WS op so
  admin-role revocation takes effect on open connections (SEC-004); strict
  OAuth-callback origin validation + JS/HTML escaping (SEC-005);
  `react-router-dom` 6.30→7.18 clearing 3 CVEs (SEC-003); upload-size hard
  ceiling + over-ceiling `maxFileSize` rejected at PATCH time (SEC-008);
  unverified-email fallback dropped (SEC-006).
- **Architecture**: `SubscriptionManager` sharded per-db (ARC-001); env-
  configurable pool size (ARC-002); CI on `make checkall` (ARC-003); typed
  protocol enums across all four clients + a cross-client wire-parity corpus
  (ARC-004/008/009/QA-008); rust-client vector `Vec<f32>`→`Vec<f64>` to match
  the wire's f64 precision (ARC-008(a)); `AppState` regrouped into sub-structs
  (ARC-006); `mutation_log` expiry moved to a background task (ARC-007).
- **Quality**: `execute_query` validation cascade refactored to a dispatch
  table (QA-002); TS in-memory `get`-guard drift fixed + a cross-client
  combination matrix (QA-001); dashboard Vitest+RTL suite (QA-003).
- **Docs**: README session-expiry contradiction fixed (DOC-001); Python client
  documented (DOC-002); `CHANGELOG`/`CONTRIBUTING`/`LICENSE` added; design-spec
  statuses flipped to Implemented + `SPEC_STATUS.md` index.

See commit range `b0f7108..` on `main`. The three items previously tracked as
manual follow-ups — SEC-001 (HttpOnly cookie), SEC-008 (PATCH-side `maxFileSize`
check), and ARC-008(a) (`Vec<f32>`→`Vec<f64>`) — are now implemented (CI fix
`oven/setup-bun`→`oven-sh/setup-bun` landed too).

### Added

#### Server

- **Reactive live queries** over WebSocket `/sync` — push-on-change only, canonical-JSON diffing (FEATURE_MATRIX parity row 0).
- **Atomic multi-step transactions** — declarative DSL: `insert`/`patch`/`replace`/`delete`/`upsert` + `expectVersion`/`expectAbsent` preconditions; serialized through a single-writer committer per database.
- **Typed schema** — `string, number, boolean, null, id, literal, optional, union, array, object`.
- **Secondary and compound indexes** — real Postgres btree per index, `_creationTime` tiebreaker matching Convex ordering.
- **Query surface**: `get`, index-prefix `eq`, `order`, `take`, `collect`, `unique`.
- **System fields** — `_id`, `_creationTime`, and `_version` (powers client-side OCC that Convex doesn't expose).
- **HTTP one-shot** query/mutate with per-database machine tokens (`POST /api/query`, `POST /api/mutate`).
- **Multi-provider OAuth** (GitHub + Google) with cross-provider same-email linking and per-database email allowlists (FEATURE_MATRIX #14).
- **Live permission revocation** — `authorize` re-runs on every WebSocket Subscribe/Mutate; machine-token revocation, allowlist removal, session expiry, and admin-role revocation take effect on open connections (#8). Admin `is_admin` is also re-run per WS op.
- **Range queries** — `gt`/`gte`/`lt`/`lte` after the `eq` prefix (#1).
- **`first()`** terminal — sugar over `take(1)` (#2).
- **`count()`** terminal — uncapped `SELECT COUNT(*)` over the eq-prefix + range-bound WHERE clause (#3).
- **`replace`** step — full-document overwrite (#6).
- **Safe mutation retry** via opt-in idempotency keys (`idempotencyKey` on both transports); 5-minute TTL (#4).
- **Pagination** — keyset pagination via opaque base64 cursor over the full sort-column tuple; `usePaginatedQuery` hook in the TS client (#5).
- **Snapshot export/import per database** — `GET /admin/export-db`, `POST /admin/import-db` (#7).
- **Scheduled transactions** — `afterMs`/`runAt` one-shot, `cron` recurring; per-db `scheduled_txns` side table drained through the committer by a per-db scheduler; at-least-once, no-backfill cron (#9, #10).
- **Full-text search** — declared search index compiles to a generated tsvector column + GIN index; `search` query terminal ranks by `ts_rank`; bound `plainto_tsquery`, no tsquery-syntax injection (#11).
- **db-side `filter()`** expressions — `eq`/`neq`/`gt`/`gte`/`lt`/`lte`/`in` + `and`/`or` combinators compiled to SQL (#15).
- **File storage** — Postgres-native blobs (per-db `storage` table + global `storage_index`); `POST /api/storage/{db}` upload, `GET /storage/{id}` unauthenticated public serve, authed serve/metadata/delete (#16). HTTP-only, bypasses the committer.
- **Vector search** — pgvector extension; `Vector` field type, write-maintained `vector(N)` column, HNSW `vector_cosine_ops` index, `vectorSearch` terminal with optional eq-`filter` over declared `filterFields`; client-supplied embeddings, no server-side generation; live in dev and prod (`pgvector/pgvector:pg17`, vector 0.8.5 — verified 2026-07-25) (#17).
- **Per-row authorization** — opt-in `ownerField`, enforced on every read terminal, every `fan_out` (subscriber's owner captured at subscribe time), and every write (auto-stamp on insert + in-txn ownership pre-check on patch/replace/delete/upsert-update → `Forbidden`/403); immutable post-insert (#20).
- **Fine-grained subscription invalidation** — `get(id)` point reads skip re-runs when the write didn't touch their document; `count`/`collect`/`unique` on a btree index's eq-prefix (+ optional range bound) skip when every written doc is provably outside their window (`WriteSet.doc_values` carries before/after; never under-approximates — deletes always re-run); `take(N)`/`first`/`paginate` skip when every written doc is outside that window *or* ranks beyond the last result's final row (the top-N boundary, refreshed on every re-run; an unfull result is unbounded), for which `doc_values` also carries each written doc's `created_at`; `distinct`/`aggregate`/`search`/`vector`/`hybrid` stay table-level (#21). Guarded by two safety nets, since a wrong skip is otherwise silent: `cmp_binds` is structured so a new `EqBind` variant is a compile error rather than an under-approximating fallback, and `RTDB_SUBS_VERIFY_SKIP_EVERY` (default 0 = off) shadow-verifies 1 skip in every N — a divergence logs at ERROR, increments `rtdb_subs_missed_pushes_total`, and pushes the corrected result. Skip/re-run effectiveness is counted per read-set class (`rtdb_subs_skips_total{class}`) on `/admin/metrics` + `/metrics`, mirrored in the ts and rust clients, and shown on the dashboard metrics page.
- **Operator dashboard backend** — admin allowlist CRUD (`/admin/admins`), per-db metadata (`/admin/dbs/{db}/{schema,stats}`), live metrics (`/admin/metrics`), realtime op feed (`/admin/ops/recent`, `WS /admin/stream`), hot-reloadable config (`GET/PATCH /admin/config`), admin document access (`POST /admin/db/{db}/query|mutate`, `RTDB_MAX_AFFECTED_DOCS` cap), and same-origin static SPA hosting gated on `RTDB_STATIC_DIR` (#18).
- **Extra validators** — `record`, `any`, `bytes`, `int64` (JSON-string of decimal digits, branded `Int64` on the TS client) (#13).
- **OAuth admin allowlist seed** — `RTDB_ADMIN_EMAILS` env seeds `rtdb_auth.admins` at boot.

#### TypeScript client (`@par-rt-db/client`, `ts-client/`)

- **No-codegen schema** — TS source of types; `Doc`/`Id` inferred from the schema.
- **Reactive WebSocket client** — auto-reconnect, re-auth, resubscribe, heartbeat, stale-callback generation guard.
- **React bindings** — `RtDbProvider`, `useQuery`, `useMutation`, `useConnectionState`, auth gates, `usePaginatedQuery`.
- **HTTP/admin clients** — one-shot query/mutate, schedule, storage, token mint/revoke, schema push, snapshot export/import.
- **In-memory test harness** — `InMemoryRtDbClient` mirroring server query/txn/subscription semantics offline.
- **Optimistic updates** — opt-in `optimisticUpdates` overlaid on each subscription's last result; server reconciles, rolls back on error (#12). Rust/Python pending.

#### Rust client (`par-rt-db-client`, `rust-client/`)

- Wire contract, schema/mutation/query DSL, http + reactive ws + admin clients, index helpers, `mutate_with_retry`, `.filter()`/`.search()`/`.vector_search()` builders, schedule + storage surfaces.
- Opt-in live-server integration test (`tests/http_integration.rs`, `#[ignore]`, `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`).
- Optimistic updates pending (matches FEATURE_MATRIX #12).

#### Python client (`par-rt-db`, `python-client/`)

- Wire contract — `ServerMessage` / `ClientMessage` unions, `AuthedUser`, `Schedule*`, `FilterExpr`, `SearchQuery`/`VectorSearchQuery`, `RtDbError`/`ErrorCode`.
- Schema DSL — 15 `FieldType` variants, btree/search/vector indexes, `TableBuilder`, `ownerField`.
- Mutation DSL — `Step`/`StepResult`/`Transaction`/`Mutation` builders.
- Query DSL — `TableQuery` builders (`get`/`with_index`/`eq`/`gt`/…/`order`/`take`/`unique`/`first`/`count`/`filter`/`search`/`vector_search`/`paginate`), `Query`/`QueryResult`, `encode_cursor`/`decode_cursor`.
- Four-way wire-parity fixtures (Python ↔ server ↔ TS ↔ Rust).
- HTTP/WS/admin/storage client surfaces pending.

#### Dashboard (`@par-rt-db/dashboard`, `dashboard/`)

- Operator console SPA — Vite + React 19 + TS, served same-origin at `RTDB_STATIC_DIR`.
- Three-pane "Instrument Manual" UI — admin-key + OAuth (GitHub/Google) login; databases index + per-db stats; schema spec sheet; live data browser (realtime over `/sync` for OAuth admins, ~2s polling for admin-key mode); live metrics instrument panel; op-feed page; hot-config editor; admin allowlist CRUD. Op feed + metrics stream over a single WS to `/admin/stream` (subprotocol auth).

#### Build / operations

- Root `Makefile` with `make build | fmt | fmt-check | lint | typecheck | test | checkall | dev-db-up | dev-db-down | pre-commit | pre-commit-update | deploy` spanning all five packages.
- `pre-commit` runs `gitleaks` + `detect-private-key` + format/lint checks.
- Docker deploy (`Dockerfile`, `docker-compose.yml`) — the dashboard SPA is baked into the image (`dashboard` build stage copies `dist/` to `/app/dashboard-dist`, pointed at by `RTDB_STATIC_DIR`).
- Healthz `/healthz` — `{status:"ok"|"degraded", version, git_commit, build_timestamp, started_at, uptime_seconds, postgres}`; `RTDB_BUILD_COMMIT` bake-in via build-arg for image builds without `.git`.
- Graceful shutdown on `SIGINT`/`SIGTERM` (waits for in-flight requests + open WebSockets; Docker SIGKILL is the backstop).

### Changed

- **Hot config** (`allowed_origins`, `session_ttl_days`, `max_file_size`) — runtime-mutable via `PATCH /admin/config`; persisted in `rtdb_config`, swapped live via `Arc<ArcSwap<HotConfig>>` (no restart). The CORS layer re-reads `allowed_origins` per request.
- **`GET /admin/config`** is structurally redacted — `admin_key`, OAuth secrets, and `database_url` are exposed as configured-bools only, never values.
- **C collation** — Postgres database uses deterministic C collation, eliminating collation-version warnings and making index ordering deterministic.
- **OAuth callback HTML** — strict origin validator + interpolation escaping (security hardening).

### Security

- `SEC-001` — admin token held in JS memory instead of `localStorage` in the dashboard.
- `SEC-004` — WS `is_admin` re-runs per op, closing the admin-revocation lag on open `/sync` connections.
- `SEC-005` — OAuth callback strict origin validator + escaping (self-XSS prevention).
- Admin key compared constant-time (`subtle::ConstantTimeEq`), shared by the header path and the `rtdb-admin.<token>` subprotocol path.
- Per-row `ownerField` pre-checks run inside the serialized transaction with no TOCTOU window; machine tokens and scheduled jobs bypass per-row rules but the db-level gate still runs first.
- `GET /storage/{id}` is the single unauthenticated route — opaque uuid-v7 URLs, revoke by delete, cross-db isolated via `storage_index`.

[Unreleased]: https://github.com/paulrobello/par-rt-db
