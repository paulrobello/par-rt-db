# rtdb-server

The **par-rt-db** realtime document database server — a self-hosted,
Convex-inspired document DB in Rust (axum/tokio + Postgres 17). Clients send a
declarative JSON DSL (typed queries and atomic multi-step transactions) over
WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live
query updates on change. One instance hosts many named databases. There is no
embedded JS runtime and no per-app server code — this one generic binary serves
every app.

This directory holds the `rtdb-server` binary. The three client SDKs live
alongside it: [`../ts-client/`](../ts-client) (browser/Node),
[`../rust-client/`](../rust-client) (Rust), and
[`../python-client/`](../python-client) (Python — wire + DSL + sync HTTP/admin/storage
+ reactive WS, all shipped). An operator dashboard SPA
([`../dashboard/`](../dashboard)) is served same-origin by the server when
`RTDB_STATIC_DIR` is set, and the [`../cli/`](../cli) package wraps
`par-rt-db-client` as the `rtdb` operator/CI binary. See the
[root README](../README.md) for the project overview and
[`../CLAUDE.md`](../CLAUDE.md) for contributor guidance. Authoritative design:
[`../docs/superpowers/specs/2026-07-21-par-rt-db-design.md`](../docs/superpowers/specs/2026-07-21-par-rt-db-design.md).

## Stack

- **axum 0.8** + **tokio** — HTTP and WebSocket transports, graceful shutdown.
- **sqlx 0.8** + **Postgres 17** — storage: one typed column per indexed field,
  documents stored as `doc jsonb` (system fields merged in at read time).
- **tracing** — structured logs.
- Auth: multi-provider OAuth trait (`auth/provider.rs`) — six providers ship
  behind it: GitHub (`auth/github.rs`), Google (`auth/google.rs`),
  GitLab (`auth/gitlab.rs`), Microsoft / Entra ID v2 (`auth/microsoft.rs`),
  Apple (`auth/apple.rs`, ES256 JWT `client_secret` + `response_mode=form_post`),
  and a generic OIDC provider (`auth/oidc.rs`). Cross-provider same-email logins
  link to one user by email (Apple additionally keys on its stable `sub`); a
  per-database email allowlist gates database access; the admin key is compared
  constant-time. Login-CSRF is defended by the `rtdb-oauth-csrf` double-submit
  cookie (`RTDB_OAUTH_LOGIN_CSRF=false` to disable). See
  [`../docs/OAUTH_SETUP.md`](../docs/OAUTH_SETUP.md) and FEATURE_MATRIX #14.
- Per-row authorization (FEATURE_MATRIX #20): a table may declare any of three
  opt-in rules — `ownerField` (owner-only), `collaboratorsField` (owner OR
  collaborator), and `authorize` (a general `FilterExpr` predicate over doc
  fields plus `$user`/`$email` principal markers). Enforced server-side on
  query, mutate, and subscription re-run; machine tokens and scheduled jobs
  bypass per-row rules. See
  [`../docs/superpowers/specs/2026-08-02-per-row-auth-predicate-dsl-design.md`](../docs/superpowers/specs/2026-08-02-per-row-auth-predicate-dsl-design.md).

## Layout

| Area | Files |
| --- | --- |
| Correctness core (serialized writes + subscription fan-out) | `src/committer.rs`, `src/subs.rs` |
| Scheduled / cron transactions | `src/scheduler.rs` (+ the `RunScheduled` arm in `src/committer.rs`) |
| TTL reaper | `src/reaper.rs` (+ the `RunReaper` arm in `src/committer.rs`) |
| Schema migration (destructive transforms) | `src/migrate.rs` (+ the `RunMigrate` arm in `src/committer.rs`) |
| Schema change history + restore | `src/schema_history.rs`, `src/schema_diff.rs` (+ the `RunRestoreSchema` arm in `src/committer.rs`) |
| File storage (blobs) | `src/storage.rs` (+ the storage routes in `src/http_api.rs`) |
| On-the-fly image transforms (ENH-014) | `src/image_transform.rs` (read-time, both serve routes; `moka` cache + decode semaphore + pixel cap; `RTDB_IMAGE_*` knobs) |
| Per-database resource quotas (ENH-011) | `src/quota.rs` (three global caps on `HotConfig`; tables/subs/storage enforcement; `QUOTA_EXCEEDED` 507) |
| Realtime presence (ENH-015) | `src/presence.rs` (in-memory, connection-bound, not committer-bound; `RTDB_PRESENCE_*` knobs) |
| Audit log (when `RTDB_AUDIT_LOG_ENABLED=true`) | `src/audit.rs` (best-effort row per `DocOp` at the committer tap sites) |
| Webhook outbox (when `RTDB_WEBHOOKS_ENABLED=true`) | `src/webhook.rs` (per-`DocOp` outbox row drained by a boot worker; at-least-once) |
| Backup lifecycle (when `RTDB_BACKUP_ENABLED=true`) | `src/backup.rs` (manual `pg_dump` trigger + dump list/download/delete; `pg_restore --no-owner --no-privileges` into a fresh `rtdb_restored_<stamp>` DB) |
| Rate limiter (per-token + per-db fixed window) | `src/rate_limit.rs` (`RTDB_RATE_LIMIT_PER_TOKEN_RPM` / `RTDB_RATE_LIMIT_PER_DB_RPM`, 0 = off) |
| OpenTelemetry / OTLP tracing (ENH-018, opt-in) | `src/tracing_setup.rs` (subscriber init + `OtelGuard` flush); span instrumentation in `committer.rs`/`subs.rs`/`query.rs`/`txn.rs`. The `otel` cargo feature (default off) gates the deps + subscriber; `RTDB_OTEL_ENABLED` (default false) gates it at runtime. `committer.mutate` carries `queue_wait_ms`. |
| Query introspection (ENH-019) | `POST /admin/db/{db}/explain` (re-compiles a Query JSON via `compile_query`, returns `{sql, params, terminal, warnings}` — no rows) and `GET /admin/slow-queries` (bounded ring of queries that exceeded `RTDB_SLOW_QUERY_MS`). Ring + `SlowQueryRecord` in `src/metrics.rs`; explain + slow-query recording in `src/http_api.rs`; list endpoint in `src/admin/observability.rs`. `RTDB_SLOW_QUERY_MS=0` (default) disables; `RTDB_SLOW_QUERY_LOG_PARAMS=false` (default) keeps document content out of the log. |
| Mutation-log dedup (idempotency) | `src/mutation_log.rs` |
| Op feed (in-memory ring + `/admin/stream`) | `src/op_feed.rs` |
| Snapshot export/import | `src/snapshot.rs` |
| Metrics | `src/metrics.rs` |
| Hot config + dynamic CORS | `src/config.rs` (`Arc<ArcSwap<HotConfig>>` on `AppState`) |
| Health | `src/health.rs` |
| Schema model + validation | `src/schema.rs` |
| Schema → Postgres DDL | `src/ddl.rs` |
| Write / read paths | `src/txn.rs`, `src/query.rs` |
| Pagination (cursor keyset) | `src/pagination.rs` |
| Wire messages | `src/protocol.rs` |
| Error envelope | `src/error.rs` |
| Transports | `src/ws.rs` (reactive), `src/http_api.rs` (one-shot) |
| Admin control plane | `src/admin/` — `mod.rs` (shared core + assembled router) + twelve per-domain submodules (`login`, `dbs`, `schema_ops`, `tokens`, `docs`, `schedules`, `storage_ops`, `webhooks`, `backups`, `settings`, `observability`, `sessions`); all `/admin/*` routes + `/admin/stream` WS. `sessions` is the active-session management surface (`GET/DELETE /admin/sessions`, per-user + per-token-hash revocation; revocation takes effect on the next op over an already-open connection). |
| Signed, time-limited storage URLs (ENH-017) | `src/signed_url.rs` (HMAC over `admin_key`, `?exp=&sig=` verified on `GET /storage/{id}`) |
| Auth (six OAuth providers + sessions + machine tokens + anonymous) | `src/auth/` — `mod.rs`, `provider.rs` (trait + dispatcher), `github.rs`, `google.rs`, `gitlab.rs`, `microsoft.rs` (Entra ID/Azure AD v2), `apple.rs` (ES256 JWT `client_secret` + `form_post`), `oidc.rs` (generic), `session.rs`, `tokens.rs`, `cookie.rs`. Anonymous auth (`POST /auth/anonymous`, gated `RTDB_AUTH_ANONYMOUS_ENABLED` default off) mints an ephemeral `Principal::User` (`anonymous = true`, `email = None`) that bypasses the per-db allowlist via its boot gate and owns its own documents via per-row `ownerField`. On a later OAuth sign-in, the anon footprint is merged into the real account (`src/merge.rs` — doc restamps in the committer, storage owner swap, session re-point, guarded anon-row delete); `POST /admin/merge-users` is the operator escape hatch. |

The read path compiles a db-side `filter()` predicate DSL to SQL, a full-text
`search` query terminal backed by a generated tsvector column + GIN index,
ranked by `ts_rank`, and a `vectorSearch` terminal backed by pgvector — a
write-maintained `vector(N)` column with an HNSW index using the declared
metric's opclass (`vector_cosine_ops` for cosine (default), `vector_l2_ops` for
L2, `vector_ip_ops` for inner-product), ranked by that metric's distance
operator and carrying an optional full `FilterExpr` (the same predicate DSL
`.filter()` accepts). Embeddings are client-supplied (no server-side
generation); the Postgres image is `pgvector/pgvector:pg17`. The `search`
terminal also takes an optional `mode: "trgm"` (FM-30): substring/autocomplete
matching via `pg_trgm` — case-insensitive `ILIKE '%q%'` over the index's text
fields, ranked by `similarity()`, composing with `filter` and `take`. Every
search index carries a GIN trigram index (`tg_<table>_<index>`) beside its
tsvector GIN, created on push with `IF NOT EXISTS` so existing deployments
backfill on the next schema push; the tradeoff is roughly double the index
storage over search fields. `mode` omitted (or `"tsquery"`) is today's
full-text behavior, unchanged.

## Scheduling

Per-database scheduled/cron transactions live in `src/scheduler.rs`. Each
database gets a `scheduled_txns` side table (sibling of `mutations`) holding
`(due_at, txn)` rows of declarative `Transaction`s — not code — created in
`db::create_database` and lazily by `scheduler::ensure_table`. A per-db timer
task (`scheduler::run_scheduler`, spawned alongside the committer in
`Committers::channel_for`) claims due rows and enqueues each as a
`CommitterRequest::RunScheduled`; the committer's `RunScheduled` arm
(`handle_scheduled`) executes it via the normal `execute_txn` + `subs.fan_out`
path and finalizes the row, so the single-writer invariant is intact. The
scheduler task only writes the side table (claim/reset) — it never executes
transactions itself.

`when` is one-shot (`afterMs`/`runAt`) or a 5-field cron expression (UTC,
min-first, via the `croner` crate). Delivery is **at-least-once**: a crash
between commit and finalize is recovered by `reset_running` on startup, so apps
should write idempotent scheduled txns. A past-due one-shot fires immediately
(catch-up); cron **skips** missed windows with no backfill. WS surface:
`Schedule` / `CancelSchedule` / `PauseSchedule` / `ResumeSchedule` /
`ListSchedules`. HTTP surface: `POST /api/schedule`,
`POST /api/schedule/{id}/{cancel,pause,resume}`, `POST /api/schedules`.

## File storage

Per-database blob storage lives in `src/storage.rs`. Each database gets a
`storage` side table (`bytea`, TOAST-managed) holding each file's bytes plus
`{sha256, size, content_type, created_at}`; a global `rtdb.storage_index(id →
db_name)` resolves the unauthenticated public serve URL to the owning database.

Bytes are written via `storage::put` **directly, not through the committer** —
blobs don't touch document tables or subscriptions, so the single-writer
invariant is unaffected. Storage is not reactive, so there are **no WS
variants** — the surface is HTTP only:

- `POST /api/storage/{db}` — bearer, raw body, `Content-Type` → `{ id, sha256, size, contentType }`. The route disables axum's 2 MiB default and enforces `RTDB_MAX_FILE_SIZE` (default 50 MiB); overflow is `BadRequest`.
- `GET /storage/{id}` — **unauthenticated** public serve (anyone with the opaque uuid-v7 URL; revoke by delete). Convex parity.
- `GET /api/storage/{db}/{id}` — authed serve (caller-db-scoped).
- `DELETE /api/storage/{db}/{id}` — bearer → `{ ok: true }` (idempotent; revokes the public URL).
- `GET /api/storage/{db}/{id}/metadata` — bearer → `{ id, sha256, size, contentType?, creationTime }`.

Both serve routes honor HTTP `Range` requests (`serve_bytes` in `http_api.rs`):
`Range: bytes=...` → `206 Partial Content` with `Content-Range`/`Content-Length`
plus `Accept-Ranges: bytes`; an out-of-bounds range → `416 Range Not Satisfiable`
(`Content-Range: bytes */<total>`); no/ignored range → `200` full body as before.
Single-range only (multipart/non-`bytes`/malformed ranges are ignored per RFC
7233). On-the-fly image transforms are cache-keyed whole renders, so `Range` is
skipped there. Read-path only — no committer, protocol, or WS change.

### Signed, time-limited URLs

`GET /api/storage/{db}/{id}/signed-url?ttlSeconds=3600` (bearer-authorized for
`{db}`) mints a URL that grants read access to one blob until an absolute
expiry (default 1h, max 7d). The URL is `GET /storage/{id}?exp=<unix-ms>&sig=<hex>`,
verified by an HMAC key derived from `admin_key` — no DB lookup, and a request
with no `exp`/`sig` still serves publicly as before. Signature verification
failure returns 403 `FORBIDDEN`.

`{db}` is in the path because the raw upload/serve bodies can't carry it and
session principals aren't db-scoped. The table is created eagerly in
`db::create_database` and lazily via `storage::ensure_table` at committer
startup (mirrors `mutations`/`scheduled_txns`).

## Develop

```sh
make dev-db-up        # start dev Postgres on 127.0.0.1:55434 (required for tests)
make test             # dev-db-up, then cargo test
make checkall         # fmt-check + clippy -D warnings + typecheck + test
```

Run cargo directly from this directory: `cargo build`, `cargo clippy --all-targets
--all-features -- -D warnings`, or a single test by name
(`cargo test --test txn_test upsert_multiple_matches`). Integration test
binaries mirror the modules: `txn_test`, `query_test`, `subs_test`, `ws_test`,
`http_api_test`, `oauth_test`, `admin_test`, `healthz_test`.

Tests share one Postgres instance and isolate by creating uniquely-named
databases (`t<uuid>`) — never assume exclusive access, and never drop a database
or schema you didn't create.

## Invariants

The committer is the correctness core: every write for a database flows through
one serialized committer task, and subscriptions are re-evaluated under that same
serialization. `execute_txn`/`execute_query` run under READ COMMITTED with no row
locking — never call `execute_txn` outside the committer, and never add a second
concurrent writer. Every SQL identifier is validated and double-quoted, every
value goes through a `$n` bind, and every failure is the `RtDbError` envelope
`{code, message}` (500s carry a generic message). No `unwrap()`/`expect()`
outside `#[cfg(test)]`. Full list in [`../CLAUDE.md`](../CLAUDE.md).

## Deploy

Deployed live at `rtdb.pardev.net` on host lenny2 (plain `docker compose`)
reached through a Cloudflare tunnel. Production stack and runbook:
[`../Dockerfile`](../Dockerfile), [`../docker-compose.yml`](../docker-compose.yml),
[`../deploy/README.md`](../deploy/README.md). Build on the x86_64 host, not from
an arm64 Mac.
