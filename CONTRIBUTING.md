# Contributing to par-rt-db

Thanks for contributing. This guide covers development setup, the build gate,
commit and PR conventions, and the load-bearing invariants you must preserve.
For the agent-facing companion (with the full invariant list and module-level
notes), see [`CLAUDE.md`](CLAUDE.md).

## Table of contents

- [Repository layout](#repository-layout)
- [Development setup](#development-setup)
- [The build gate](#the-build-gate)
- [Running tests](#running-tests)
- [Style and formatting](#style-and-formatting)
- [Commit messages](#commit-messages)
- [Versioning](#versioning)
- [Pre-commit hooks](#pre-commit-hooks)
- [Troubleshooting](#troubleshooting)
- [Invariants you must preserve](#invariants-you-must-preserve)
- [Pull request checklist](#pull-request-checklist)

## Repository layout

par-rt-db is a monorepo with **six packages** built from one root `Makefile`:

| Package | Path | Stack |
| --- | --- | --- |
| Server | `server/` | Rust (axum/tokio + Postgres 17) |
| TypeScript client | `ts-client/` | TS (`@par-rt-db/client`, bun) |
| Rust client | `rust-client/` | Rust (`par-rt-db-client`) |
| Python client | `python-client/` | Python (`par-rt-db`, uv) |
| Dashboard | `dashboard/` | Vite + React 19 + TS (bun) |
| `rtdb` CLI | `cli/` | Rust (`rtdb` binary, cargo; wraps `par-rt-db-client`) |

The server is the source of truth for the wire protocol and the DSL; the four
clients mirror it. [`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) tracks parity
against Convex with per-row notes on which clients mirror each feature.

## Development setup

You need `docker` (for the dev Postgres), `cargo` (Rust stable), `bun`, and
`uv` (Python). Then:

```bash
# 1. Start the dev Postgres on 127.0.0.1:55434. Required for any test run.
make dev-db-up

# 2. Install per-package dependencies (first time only).
make ts-client-install
make dashboard-install
make python-client-install
# Cargo workspaces (server/, rust-client/, cli/) have no install step — cargo fetches on first build.

# 3. Configure the minimum environment the server needs to run.
export RTDB_DATABASE_URL='postgres://rtdb:rtdb@127.0.0.1:55434/rtdb'
export RTDB_ADMIN_KEY="$(openssl rand -hex 32)"
export RTDB_PUBLIC_URL='http://localhost:8300'

# 4. Build everything once to populate ts-client/dist (the dashboard links it).
make build
# Optional: build the `rtdb` CLI binary (also built by `make build` / `make checkall`).
make rtdb-cli

# 5. Run the full gate.
make checkall
```

If `make typecheck` fails on a fresh checkout because the dashboard can't
resolve `@par-rt-db/client`, run `make ts-client-build` (or `make build`)
first — the dashboard resolves the SDK from `ts-client/dist`, which is
gitignored and rebuilt on demand.

## The build gate

`make checkall` is the **definition of done**. It must pass before commit. It
runs, across all six packages:

- `env-drift-check` — fails first if a `RTDB_*` var documented in `.env.example` or read by the server is not forwarded to the container by `docker-compose.yml`'s `environment:` block
- `fmt-check` — formatting check (cargo fmt, bun fmt-check, ruff format --check)
- `lint` — `cargo clippy --all-targets --all-features -- -D warnings`, `bun run lint`, `uv run ruff check .`
- `typecheck` — `cargo check`, `bun run typecheck`, `uv run pyright`
- `test` — `cargo test`, `bun run test`, `cargo test --all-features`, `uv run pytest -q` (auto-runs `dev-db-up` first)

The dashboard runs `vitest run` over `dashboard/src/**/*.test.tsx` (18 files as
of 2026-08-09 — page tests for every operator page plus library/component
tests). All five other packages also have substantive suites.

Never `--no-verify` past the gate. If you do, fix the gate immediately and push
the fix before anything else.

## Running tests

Tests share one Postgres instance and isolate by creating uniquely-named
databases per test case (`t<uuid>`). Never assume exclusive access, and never
drop a database or schema you didn't create.

```bash
make test                                       # whole suite (dev-db-up + tests across all 6 packages)

# Per-package:
cd server && cargo test                         # server tests
cd ts-client && bunx vitest run                 # ts-client tests
cd rust-client && cargo test --all-features     # rust-client tests
cd dashboard && bun run test                    # dashboard tests (vitest, 18 files)
cd python-client && uv run pytest -q            # python-client tests
cd cli && cargo test                            # cli tests (rtdb binary)

# Single test:
cargo test --test txn_test upsert_multiple_matches   # one integration binary, by name
cargo test upsert                                     # by name across binaries
cd ts-client && bunx vitest run tests/<file>.test.ts
cd python-client && uv run pytest -q tests/<file>.py
```

Live-server tests are opt-in and `#[ignore]` by default:
`ts-client/tests/integration/**` and `rust-client/tests/http_integration.rs`
need `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY` and run with `--ignored`.
The rust-client one needs no dev Postgres.

## Style and formatting

Formatting is enforced by the gate (`make fmt-check`); run `make fmt` to fix
everything at once. Per-package formatters: `cargo fmt` (server + rust-client),
`bun run fmt` (ts-client + dashboard), `uv run ruff format .` (python-client).

Other rules:

- No `unwrap()`/`expect()` outside `#[cfg(test)]` (Rust).
- Zero clippy warnings under `-D warnings`.
- Match the surrounding code's density. Comments should state constraints the
  code can't show, not narrate what the next line does.

## Commit messages

This project uses **Conventional Commits** — every commit in `git log` follows
this shape, and PRs should match. The format:

```text
<type>(<scope>): <short imperative summary>

<optional body, wrapped at ~72 chars, explaining why>
```

Common `<type>` values in this repo: `feat`, `fix`, `docs`, `style`, `refactor`,
`test`, `chore`, `build`, `ci`, `perf`, `security`. `<scope>` is the package or
area (`server`, `ts-client`, `rust-client`, `python-client`, `dashboard`,
`audit`, `env`, `deploy`, `spec`, `plan`, etc.).

Examples from the log:

```text
feat(server): scheduled transactions — per-db scheduled_txns table + scheduler timer
fix(security): SEC-004 re-run is_admin per WS op, SEC-001 in-memory admin token
docs(audit): comprehensive project audit — 55 findings across 4 domains
build: wire python-client into root make checkall
```

Keep the subject line to one imperative sentence. Reference the FEATURE_MATRIX
row or audit finding ID when the change implements one (`(#18)`, `(SEC-004)`,
etc.).

## Versioning

All packages — `server`, `cli`, `dashboard`, and the three client SDKs
(`ts-client`, `rust-client`, `python-client`) — version in **lockstep**: one
version for the whole protocol surface, bumped together in the same release
commit. The four implementations of the wire contract (`server/src/protocol.rs`
plus the three client mirrors) are one unit; independent client versions would
claim compatibility the four-way mirror does not have. The release procedure,
including the lockstep bump, is [`docs/RELEASING.md`](docs/RELEASING.md).

## Pre-commit hooks

This repo uses [`pre-commit`](https://pre-commit.com) with secret scanning as a
hard gate. Install the hooks once after cloning:

```bash
pip install pre-commit      # or: brew install pre-commit
pre-commit install
make pre-commit-update      # periodically refresh hook versions
```

The configured hooks include:

- **`gitleaks`** — scans every staged change for credentials, API keys, tokens, and private keys. A hit blocks the commit.
- **`detect-private-key`** — belt-and-suspenders for private key blocks.
- Format and lint checks per language.

You can run the full hook set on demand: `make pre-commit`. Never commit a
real secret — if one slips in, force-push to remove it from history, rotate
the secret, and audit logs for misuse.

## Troubleshooting

Common contributor symptoms:

- **`make test` fails to connect to Postgres on `127.0.0.1:55434`, or two
  worktrees collide on the port.** The dev Postgres is shared across worktrees,
  so only one `make dev-db-up` can hold port `55434` at a time. If you work in a
  git worktree, reuse the already-running dev Postgres from the main checkout
  rather than starting a second one. `docker ps` shows whether it is up; the
  integration tests hit it directly, never an exclusive instance.
- **`make typecheck` fails on a fresh checkout because the dashboard can't
  resolve `@par-rt-db/client`.** The dashboard links the SDK from
  `ts-client/dist`, which is gitignored and rebuilt on demand. Run
  `make ts-client-build` (or `make build`) first to populate `dist`.
- **`uv run pyright` reports missing stubs/dependencies in `python-client/`.**
  The type checker needs the optional extras installed. Run
  `uv sync --all-extras` in `python-client/` so the `[http]`/`[aio]`/`[ws]`
  extras and their deps (`httpx`, `websockets`) are visible to pyright, not just
  the default dev group.
- **`git commit` is killed mid-hook (staged but not committed).** The
  pre-commit clippy pass can take longer than a short shell timeout. Re-run the
  commit with a longer timeout (e.g. 600000 ms) rather than re-running it
  unchanged; the staged changes are intact and recoverable. Do not
  `--no-verify` past it — run `make checkall` and fix the underlying warning.
- **Dev Postgres accumulates leaked test schemas (`db_t<uuid-v7>`).** Tests
  create uniquely-named databases per test for fast isolation and don't drop
  them afterward. Over time these bloat `pg_dump` and slow the dev DB. Run
  `make dev-db-clean` periodically to drop them (the cleanup is scoped to the
  `db_t*` pattern and never touches `rtdb`/`rtdb_auth`/real databases).

## Invariants you must preserve

These are the load-bearing rules. Violating them silently is worse than a
failing test.

- **Single-writer committer** — every write for a database flows through one
  serialized committer task; subscriptions re-run under the same
  serialization. Never call `execute_txn` outside the committer, and never add
  a second writer. Reads run under READ COMMITTED with no row locking.
- **SQL construction** — validate and double-quote every identifier; bind
  every value via `$n`. Never interpolate an unvalidated value. Physical names
  are lowercased and length-capped to fit Postgres's 63-byte limit — don't
  raise the caps.
- **Four clients, one wire contract** — `server/src/protocol.rs`,
  `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, and
  `python-client/src/par_rt_db/wire.py` are four implementations of the same
  protocol. The casing is deliberately non-uniform and load-bearing. Any
  server change must be mirrored in **all four** — wire types, DSL builders,
  and their tests. If a client doesn't yet cover a changed surface, file the
  gap explicitly in `FEATURE_MATRIX.md` rather than letting it drift.
- **Errors** — every failure is the `RtDbError` envelope `{code, message}`
  (codes/statuses in `error.rs`). Client-facing 500s carry a **generic**
  message — never stringify a sqlx/serde error into the body (log it via
  `tracing`). Use `fetch_optional` for any lookup that can legitimately miss.
- **Live authz** — `authorize` re-runs on every WS Subscribe/Mutate and
  `is_admin` re-runs on every admin op. Don't add a cached auth check that
  bypasses this.
- **Op-feed tap** — durable document mutations publish through the committer's
  single enforcement point: every `handle_*` arm in `committer.rs` that commits
  a document txn calls `publish_taps` — currently seven (`handle_mutate`,
  `handle_scheduled`, `handle_migrate`, `handle_reaper`,
  `handle_restore_schema`, `handle_merge_users`, `handle_workflow_advance`;
  see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the canonical list and
  each arm's `source` tag). Any new code path that commits a document txn must
  publish through `publish_taps` the same way, or the op-feed (and
  `/admin/stream`, the audit log, and the webhook outbox) will silently miss
  those writes.
- **Storage bypasses the committer** — `storage::put` writes directly to the
  `storage` side table because blobs don't touch document tables or
  subscriptions. `GET /storage/{id}` is the only unauthenticated route; keep
  it that way.
- **`GET /admin/config` is redacted** — `admin_key`, OAuth secrets, and
  `database_url` are exposed as configured-bools, never values. Keep new
  secrets out of this response.
- **Static SPA hosting is the last fallback** — the `ServeDir` is the router's
  last `fallback_service`, so it can never shadow `/healthz`, `/api/*`,
  `/admin/*`, `/sync`, or `/auth/*`.

### Keep docs in sync

When a feature lands or changes, update in the same PR:

- [`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) — flip rows ❌→✅, note client-mirror status, bump counts.
- The relevant README(s) (`README.md`, `server/README.md`, `ts-client/README.md`, `rust-client/README.md`, `python-client/README.md`, `dashboard/README.md`, `cli/README.md`, `deploy/README.md`).
- [`CHANGELOG.md`](CHANGELOG.md) — add an entry under `[Unreleased]`.
- [`docs/DOCUMENTATION_STYLE_GUIDE.md`](docs/DOCUMENTATION_STYLE_GUIDE.md) — the canonical style guide for all par-rt-db documentation (formatting, headings, tone, code-block conventions). Follow it for any documentation change.
- Any skill that documents par-rt-db's surface.

A stale doc that contradicts the code is a bug.

## Pull request checklist

Before requesting review:

- [ ] `make checkall` passes locally on a clean checkout.
- [ ] Tests are added or updated for any new behavior (every package the change touches).
- [ ] If the change alters the wire protocol or DSL, all four clients (`server`, `ts-client`, `rust-client`, `python-client`) are updated and tested in the same PR, and `FEATURE_MATRIX.md` reflects the new state.
- [ ] No `unwrap()`/`expect()` outside `#[cfg(test)]`; no new clippy warnings.
- [ ] No real secrets in the diff. `pre-commit` (gitleaks) is installed and passes.
- [ ] Commit messages follow Conventional Commits.
- [ ] Relevant docs (`README`, `FEATURE_MATRIX`, `CHANGELOG`, package READMEs) are updated.
- [ ] If the change adds or renames an env var, `.env.example`, `docker-compose.yml`'s `environment:` block (the allowlist that actually feeds the container), and the root README Configuration table are updated — `make env-drift-check` (first stage of `checkall`) guards this.
- [ ] If the change adds a route, the root README Endpoints table and `server/README.md` are updated.
- [ ] If the change adds or modifies a query terminal, `FilterExpr` variant, or mutation-step result shape, a case covering it is added to `wire-corpus/golden-vector.json` (the cross-client parity fixture consumed by all four golden-vector suites). The corpus is the source of truth for cross-client agreement; an uncovered terminal is a regression risk for the other three engines.
- [ ] If the change alters any server behavior a client engine mirrors — step results, error codes, defaults, visibility (soft delete/TTL), cursor semantics — a case pinning the new behavior is added (or updated) in `wire-corpus/semantics/` per the authoring rule in [`wire-corpus/README.md`](wire-corpus/README.md).
- [ ] If a property test (`server/tests/proptest_parity.rs`) found a server-vs-engine divergence, the fix also ships the minimized counterexample as a `wire-corpus/semantics/` case (so all three client engines inherit it, not just the rust one — unmirrored engines get a loud `skip` until they do) and commits the proptest seed under `server/proptest-regressions/`.

When the PR lands, **rebase onto the latest target branch before merging** so
the merge is a clean fast-forward. Squash-merge one commit per logical change;
keep the Conventional Commit subject.
