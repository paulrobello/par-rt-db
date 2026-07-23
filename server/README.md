# rtdb-server

The **par-rt-db** realtime document database server — a self-hosted,
Convex-inspired document DB in Rust (axum/tokio + Postgres 17). Clients send a
declarative JSON DSL (typed queries and atomic multi-step transactions) over
WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live
query updates on change. One instance hosts many named databases. There is no
embedded JS runtime and no per-app server code — this one generic binary serves
every app.

This directory holds the `rtdb-server` binary. The two client SDKs live
alongside it: [`../ts-client/`](../ts-client) (browser/Node) and
[`../rust-client/`](../rust-client) (Rust). See the [root README](../README.md)
for the project overview and [`../CLAUDE.md`](../CLAUDE.md) for contributor
guidance. Authoritative design:
[`../docs/superpowers/specs/2026-07-21-par-rt-db-design.md`](../docs/superpowers/specs/2026-07-21-par-rt-db-design.md).

## Stack

- **axum 0.8** + **tokio** — HTTP and WebSocket transports, graceful shutdown.
- **sqlx 0.8** + **Postgres 17** — storage: one typed column per indexed field,
  documents stored as `doc jsonb` (system fields merged in at read time).
- **tracing** — structured logs.
- Auth: GitHub OAuth (`auth/github.rs`) + hashed per-database machine tokens
  (`auth/tokens.rs`); the admin key is compared constant-time.

## Layout

| Area | Files |
| --- | --- |
| Correctness core (serialized writes + subscription fan-out) | `src/committer.rs`, `src/subs.rs` |
| Schema model + validation | `src/schema.rs` |
| Schema → Postgres DDL | `src/ddl.rs` |
| Write / read paths | `src/txn.rs`, `src/query.rs` |
| Wire messages | `src/protocol.rs` |
| Transports | `src/ws.rs` (reactive), `src/http_api.rs` (one-shot) |
| Auth | `src/auth/` (`tokens.rs`, `github.rs`, `session.rs`) |

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
