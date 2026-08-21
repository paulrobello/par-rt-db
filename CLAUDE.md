# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What this is

par-rt-db is a self-hosted, Convex-inspired realtime document database in Rust (axum/tokio + Postgres 17). Clients send a **declarative JSON DSL** — typed queries and atomic multi-step transactions — over WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live query updates on change. One instance hosts many named databases. There is **no embedded JS runtime** and **no per-app server code** — one generic server serves every app.

Authoritative sources: the [spec](docs/superpowers/specs/2026-07-21-par-rt-db-design.md) for protocol and semantics (read it before changing either), [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for server internals (the committer, background tasks, auth, storage — with the reasoning behind each invariant), [README.md](README.md) for the HTTP/WS surface and configuration, and [FEATURE_MATRIX.md](FEATURE_MATRIX.md) for the Convex-parity contract.

## Workspace & commands

Seven packages run from the root `Makefile` (swift-client's lines are Darwin-guarded — they echo a loud skip on Linux, and a macOS CI job runs `make swift-client-checkall`):

| Package | Path | Tool |
| --- | --- | --- |
| Server — the realtime DB binary | `server/` | cargo |
| TypeScript client — `@par-rt-db/client` | `ts-client/` | bun |
| Rust client — `par-rt-db-client` | `rust-client/` | cargo |
| Python client — `par-rt-db` | `python-client/` | uv |
| Swift client — `ParRtDbClient`/`ParRtDbUI` | `swift-client/` | swift |
| Operator dashboard SPA | `dashboard/` | bun (Vite + React) |
| `rtdb` CLI — wraps the rust client | `cli/` | cargo |

- `make checkall` — the full gate (fmt-check + clippy `-D warnings` + typecheck + tests). **Definition of done; must pass before commit.**
- `make dev-db-up` / `dev-db-down` — start/stop the dev Postgres on `127.0.0.1:55434`. **Required for any test run** — integration tests hit a real DB. `make dev-db-clean` periodically drops leaked test artifacts (per-test `db_t…` schemas and the corpus runner's `sc_…` databases; scoped to those patterns, never touches real DBs).
- `make test` — dev-db-up then the whole suite. First-time setup: `make ts-client-install`, `make dashboard-install`, `make python-client-install`.
- Single test: `cargo test --test txn_test upsert_multiple_matches` (from `server/`), `cd ts-client && bunx vitest run tests/<file>.test.ts`, or `cd python-client && uv run pytest -q tests/<file>.py`.
- `build` and `typecheck` pull `ts-client-build` first — the dashboard resolves `@par-rt-db/client` from `ts-client/dist` (gitignored); build it on a fresh or stale checkout or the gate fails at dashboard typecheck.
- Live-server tests are opt-in (`#[ignore]`, need `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`, run with `--ignored`).

Tests share one Postgres, isolating via uniquely-named databases per test. Never assume exclusive access, and never drop a database or schema you didn't create.

## Architecture — high-level map

Full detail and reasoning: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). The one load-bearing fact: **each database has a single serialized committer task** (`committer.rs` + `subs.rs`) — all writes flow through it, then it re-runs affected subscriptions and pushes only on change. Reads run READ COMMITTED with no row locking, so this serialization is what makes correctness hold. **Never call `execute_txn` outside the committer; never add a second writer.**

- **Per-db background tasks** (`scheduler.rs`, `workflows.rs`, `reaper.rs`): scheduler, TTL reaper, and mutation-log cleanup never write document tables — they only claim/enqueue work back through committer request arms (`RunScheduled` / `RunWorkflowAdvance` / `RunReaper`, plus `RunMigrate` for schema migrate).
- **Data pipeline** (`schema.rs` → `ddl.rs` → `txn.rs`/`query/`): pushed schemas compile to Postgres DDL (one typed column per indexed field + `doc` jsonb, additive-only changes); transactions are ordered step lists with per-step caps; the read and write paths share index-value typing — keep them aligned.
- **Two transports, one vocabulary** (`protocol.rs`, `ws.rs`, `http_api.rs`): both route mutations through `Committers::mutate` so subscriptions fire regardless of which transport wrote.
- **Auth** (`auth/`): per-db machine tokens, OAuth sessions (six providers — see `docs/OAUTH_SETUP.md`), optional anonymous. The WS handler re-runs `authorize` on every Subscribe and Mutate so revocation, allowlist changes, and session expiry take effect on open connections. Opt-in per-row rules: `ownerField` / `collaboratorsField` / `authorize` predicate DSL.
- **File storage** (`storage.rs`): HTTP-only and bypasses the committer (blobs touch no document tables); `GET /storage/{id}` is the one unauthenticated route. Image transforms, signed URLs, and Range requests are read-time capabilities on the serve routes.
- **Quotas** (`quota.rs`): optional per-db caps (tables / storage / subs) enforced hard — no admin bypass; raise a cap via `PATCH /admin/config`.
- **Wire contract**: `server/src/protocol.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`, and `swift-client/Sources/ParRtDbClient/Wire.swift` (with the query/txn wire structs in `Query.swift`/`Mutation.swift` alongside it) are five implementations of one protocol and must stay byte-identical (serde tags and field names — the casing is deliberately non-uniform and load-bearing). The SDKs are no-codegen: a schema object is both pushed to the server and the source of inferred types.
- **Dashboard SPA** (`lib.rs`): served same-origin from `RTDB_STATIC_DIR` as the router's last fallback — it can never shadow API routes.

## Invariants you must preserve

- **SQL construction**: validate and double-quote every identifier; bind every value via `$n`. Never interpolate an unvalidated value. Physical names are lowercased and length-capped to fit Postgres's 63-byte limit (see `ddl.rs`) — don't raise the caps.
- **Errors**: every failure is the `RtDbError` envelope `{code, message}` (codes/statuses in `error.rs`). Client-facing 500s carry a **generic** message — never stringify a sqlx/serde error into the body (log it via `tracing`). Use `fetch_optional` for any lookup that can legitimately miss.
- **Op-feed tap**: every code path that commits a document txn must go through a committer `handle_*` arm calling `publish_taps` (`committer.rs`; the arms are enumerated in ARCHITECTURE.md), or the op-feed, audit log, and webhooks will silently miss those writes. TTL deletes are durable writes the same way.
- **Clients mirror the core**: the server is the source of truth for the protocol, DSL, step-result shapes, and behavior. Any server change must be mirrored in **all four** clients (ts-client, rust-client, python-client, swift-client) — wire types, DSL builders, and their tests. If a client doesn't yet cover a changed surface, file the gap explicitly rather than letting it drift. The [wire-corpus](wire-corpus/README.md) semantics corpus enforces this — all five runners (server + four in-memory engines, swift's engine shipped 2026-08-19) execute every case, and every behavior-changing change ships with a case (its README's authoring rule).
- **Backups never touch the live DB**: restore goes into a fresh `rtdb_restored_<stamp>` Postgres DB (`backup.rs`, `admin/backups.rs`); the single-writer invariant is preserved. Credentials travel via `PG*` env, never argv.
- **Hot config is live**: runtime-mutable settings (`allowed_origins`, `session_ttl_days`, `max_file_size`, `idempotency_ttl_ms`, quota caps) live on `AppState` as `Arc<ArcSwap<HotConfig>>` (`config.rs`); every consumer reads `state.hot.load()`, and the CORS layer re-reads `allowed_origins` per request.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Keep docs in sync**: when a feature lands or changes, update `FEATURE_MATRIX.md` (the Convex-parity contract), the relevant README(s)/docs, and any skill that documents par-rt-db's surface. A stale doc that contradicts the code is a bug.

## Deployment

Production runs as plain `docker compose` on a standalone Docker host behind a Cloudflare tunnel (deploy target set by `DEPLOY_HOST`; runbook: `deploy/README.md`). **Build on the x86_64 host, not from an arm64 Mac.** Secrets come from a mode-600 `.env` (`.env.example` is the template). A new `RTDB_*` env var must be added to both `.env.example` and `docker-compose.yml`'s environment block, or `make checkall` fails at env-drift-check.
