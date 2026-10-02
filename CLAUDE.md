# CLAUDE.md

## What this is

par-rt-db is a self-hosted, Convex-inspired realtime document database in Rust (axum/tokio + Postgres 17). Clients send a **declarative JSON DSL** — typed queries and atomic multi-step transactions — over WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live query updates on change. One instance hosts many named databases. There is **no embedded JS runtime** and **no per-app server code** — one generic server serves every app.

Authoritative sources: [README.md](README.md) for the HTTP/WS surface, the DSL, and configuration; [FEATURE_MATRIX.md](FEATURE_MATRIX.md) for the Convex-parity contract; [wire-corpus/README.md](wire-corpus/README.md) for the executable semantics corpus that pins protocol behavior across all six implementations; and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for server internals (the committer, background tasks, auth, storage — with the reasoning behind each invariant). The [2026-07-21 design spec](docs/superpowers/specs/2026-07-21-par-rt-db-design.md) is historical and superseded by those four.

## Workspace & commands

Nine packages run from the root `Makefile` (swift-client's lines are Darwin-guarded; a macOS CI job runs `make swift-client-checkall`):

| Package | Path | Tool |
| --- | --- | --- |
| Core — `par-rt-db-core`, the shared wire vocabulary | `core/` | cargo |
| Server — the realtime DB binary | `server/` | cargo |
| TypeScript client — `@par-rt-db/client` | `ts-client/` | bun |
| Rust client — `par-rt-db-client` | `rust-client/` | cargo |
| Python client — `par-rt-db` | `python-client/` | uv |
| Swift client — `ParRtDbClient`/`ParRtDbUI` | `swift-client/` | swift |
| Go client | `go-client/` | go |
| Operator dashboard SPA | `dashboard/` | bun (Vite + React) |
| `rtdb` CLI — wraps the rust client | `cli/` | cargo |

- `make checkall` — the full gate (`fmt`, `lint`, `typecheck`, `test`, and drift checks). **Definition of done; must pass before commit.** Stage details: [README.md](README.md#verification-gates).
- `make dev-db-up` / `dev-db-down` — the dev Postgres on `127.0.0.1:55434`. **Required for any test run.** `make dev-db-clean` drops leaked test artifacts (safe, pattern-scoped).
- `make test` — dev-db-up then the whole suite. First-time setup: `make ts-client-install`, `make dashboard-install`, `make python-client-install`, `make go-client-install`.
- Single test: `cargo test --test main <file_stem>::<test_name>` from `server/` (all integration tests are one binary, `tests/main.rs` — a new test file needs a `mod` line there); `bunx vitest run` in `ts-client/`; `uv run pytest` in `python-client/`.
- `build` and `typecheck` pull `ts-client-build` first — the dashboard resolves `@par-rt-db/client` from `ts-client/dist` (gitignored); build it on a fresh or stale checkout or the gate fails at dashboard typecheck.
- Live-server tests are opt-in (`#[ignore]`, need `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`).

Tests share one Postgres, isolating via uniquely-named databases per test. Never assume exclusive access, and never drop a database or schema you didn't create.

## Architecture — high-level map

Full detail and reasoning: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). The one load-bearing fact: **each database has a single serialized committer task** (`committer/` + `subs.rs`) — all writes flow through it, then it re-runs affected subscriptions and pushes only on change. Reads run READ COMMITTED with no row locking, so this serialization is what makes correctness hold. **Never call `execute_txn` outside the committer; never add a second writer.**

- **Per-db background tasks** (scheduler, workflows, TTL reaper) never write document tables — they only enqueue work back through committer request arms.
- **Data pipeline** (`schema.rs` → `ddl.rs` → `txn.rs`/`query/`): pushed schemas compile to Postgres DDL (one typed column per indexed field + `doc` jsonb, additive-only changes); the read and write paths share index-value typing — keep them aligned.
- **Two transports, one vocabulary** (`protocol.rs`, `ws.rs`, `http_api.rs`): both route mutations through `Committers::mutate` so subscriptions fire regardless of which transport wrote.
- **Auth** (`auth/`): per-db machine tokens, OAuth sessions (see `docs/OAUTH_SETUP.md`), optional anonymous; per-row rules via `ownerField` / `collaboratorsField` / `authorize`. The WS handler re-runs `authorize` on every Subscribe and Mutate, so revocation and session expiry take effect on open connections.
- **File storage** (`storage.rs`): HTTP-only, bypasses the committer (blobs touch no document tables); `GET /storage/{id}` is the one unauthenticated route.
- **Quotas** (`quota.rs`): optional per-db caps enforced hard — no admin bypass; raise via `PATCH /admin/config`.
- **Wire contract**: `server/src/protocol.rs` plus one wire module per client (`ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`, `swift-client/.../Wire.swift`, `go-client/wire/`) are six implementations of one protocol and must stay byte-identical (serde tags and field names — the casing is deliberately non-uniform). The SDKs are no-codegen: a schema object is both pushed to the server and the source of inferred types.
- **Dashboard SPA**: served same-origin from `RTDB_STATIC_DIR` as the router's last fallback — it can never shadow API routes.

## Invariants you must preserve

- **SQL construction**: validate and double-quote every identifier; bind every value via `$n`. Never interpolate an unvalidated value. Physical names are lowercased and length-capped to Postgres's 63-byte limit (`ddl.rs`) — don't raise the caps.
- **Errors**: every failure is the `RtDbError` envelope `{code, message}` (codes in `error.rs`). Client-facing 500s carry a **generic** message — never stringify a sqlx/serde error into the body (log it via `tracing`). Use `fetch_optional` for any lookup that can legitimately miss.
- **Op-feed tap**: every code path that commits a document txn must go through a committer `handle_*` arm calling `publish_taps` (`committer/taps.rs`), or the op-feed, audit log, and webhooks will silently miss those writes. TTL deletes are durable writes the same way.
- **The ownership lease is the multi-writer boundary**: under `RTDB_MULTI_INSTANCE`, exactly one replica holds the advisory-lock lease and runs its committer; every other replica is a SHADOW that forwards writes to the owner and never executes locally. **Never bypass `Committers::submit`** — it routes each write to the owner or the forward path.
- **Clients mirror the core**: the server is the source of truth for the protocol, DSL, step-result shapes, and behavior. Any server change must be mirrored in **all five** clients — wire types, DSL builders, and their tests; file any gap explicitly rather than letting it drift. The wire-corpus enforces this: all six runners execute every case, and every behavior-changing change ships with a case (its README's authoring rule).
- **Backups never touch the live DB**: restore goes into a fresh `rtdb_restored_<stamp>` database; credentials travel via `PG*` env, never argv.
- **Hot config is live**: runtime-mutable settings live on `AppState` as `Arc<ArcSwap<HotConfig>>` (`config/`, hot row in `config/hot.rs`); every consumer reads `state.runtime.hot.load()`.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`, enforced by clippy. Zero clippy warnings under `-D warnings`.
- **Keep docs in sync**: when a feature lands or changes, update `FEATURE_MATRIX.md`, the relevant README(s)/docs, and any skill that documents par-rt-db's surface. A stale doc that contradicts the code is a bug.

## Deployment

Production runs as plain `docker compose` on a standalone Docker host behind a Cloudflare tunnel (runbook: `deploy/README.md`). **Build on the x86_64 host, not from an arm64 Mac.** Secrets come from a mode-600 `.env` (`.env.example` is the template). A new `RTDB_*` env var must be added to both `.env.example` and `docker-compose.yml`'s environment block, or `make checkall` fails at env-drift-check.
