# ENH-033 — Performance benchmark harness with CI regression tracking

## Goal

The repo has no benchmarks: `server/benches/` and `rust-client/benches/` do not exist and neither
`Cargo.toml` declares `criterion`. Several audit findings are performance-shaped (ARC-006 per-op
`pg_notify` inside the committer turn, ARC-007 a synchronous UPSERT per request in multi-instance
mode, ARC-008 unbounded forward tasks, `execute_txn` and `apply_schema_additive` as hotspots) and
none can be evaluated without numbers. Add two layers: micro-benchmarks (criterion) for the pure
hot paths, and a black-box load script that drives a running server over HTTP + WS and reports
committer throughput, subscription fan-out latency, and forward round-trip time. Publish results as
a CI artifact and fail on a regression beyond a threshold against `main`'s stored baseline.

## Current state

- Hot paths (par-mem hotspots, 60-day window): `txn::execute_txn` (CC 39), `ddl::apply_schema_additive`
  (57), `migrate::validate_one` (55), `query/terminals.rs::compile_query` (49), `eval_value_expr`
  (53), the engine `execute_step`s.
- Admin observability already exposes `/admin/stats` with subscription rerun ratios (ENH-024) and
  OTLP tracing (ENH-018) — the load script can read rerun ratio and p99 from there instead of
  reinventing metrics.
- Live tests are env-gated (`RTDB_TEST_SERVER_URL`, `RTDB_TEST_ADMIN_KEY`), the established way to
  run against a real server.

## Implementation

1. **Criterion micro-benchmarks** (`server/benches/`):
   - `compile_query.rs`: compile a fixed set of 20 representative queries (filters to depth 3,
     order, search, vector) — measures the SQL compiler only, no DB.
   - `value_expr.rs`: `eval_value_expr` over the computed-field corpus expressions.
   - `validate_doc.rs`: schema validation of 1 KB / 10 KB documents.
   - `migrate_validate.rs`: `validate_one` over a 30-table schema with 8 directives.
   Add `criterion = { version = "0.5", features = ["html_reports"] }` as a dev-dep and `[[bench]]`
   entries with `harness = false`. These need no Postgres.
2. **Engine benchmarks** (`rust-client/benches/in_memory.rs`): `execute_step` and query execution
   over a 10k-row in-memory table; useful because the engine is the client-side optimistic path.
3. **Black-box load script** (`scripts/bench/load.rs` as a small binary in `cli/` behind a
   `bench` feature, or `scripts/bench/load.ts` under bun using `@par-rt-db/client` — bun keeps it
   out of the Rust build; pick bun):
   - Scenarios: (a) N writers inserting 1 KB docs for 30 s → commits/s and p50/p99 commit latency;
     (b) M subscribers on one query + one writer → time from commit to `queryUpdate` receipt p99;
     (c) multi-instance: two servers on :8300/:8301 with `RTDB_MULTI_INSTANCE=true`, writers on the
     non-owner → forward round-trip p99 and takeover time when the owner is SIGKILLed;
     (d) bulk `deleteByQuery` of 5k rows with 100 subscribers → turn hold time (ARC-006's metric).
   - Output: one JSON file per run (`bench/results/<git-sha>.json`) with the numbers above plus
     `/admin/stats` rerun ratio.
   - `make bench` starts the dev DB, `cargo run --release` on :8300 (and :8301 for scenario c),
     runs the script, stops the servers. Bounded by a deadline; never left running.
4. **Baseline + regression check**: `scripts/bench/compare.ts` reads `bench/baseline.json`
   (committed, updated deliberately by a human via `make bench-baseline`) and the new result; exits
   1 if any metric regresses more than 15% (latencies up, throughput down). Criterion has its own
   `--baseline` mechanism; use `cargo bench -- --save-baseline main` on main and
   `--baseline main` on branches.
5. **CI**: a `bench` job on `workflow_dispatch` and on `push` to `main` (not on every PR — the
   dev-DB startup and 2-minute runs are too slow for the PR gate) that uploads the JSON and the
   criterion HTML report as artifacts and comments a summary table. Runner noise is real; the 15%
   threshold and a "run twice, take the best" rule keep false alarms down.
6. Docs: `CONTRIBUTING.md` "Benchmarks" section; `docs/ARCHITECTURE.md` gets a short "Performance
   characteristics" paragraph quoting the baseline numbers with the date and hardware.

## Files to touch

- `server/Cargo.toml`, `server/benches/{compile_query,value_expr,validate_doc,migrate_validate}.rs` (new)
- `rust-client/Cargo.toml`, `rust-client/benches/in_memory.rs` (new)
- `scripts/bench/{load.ts,compare.ts}` (new), `bench/baseline.json` (new), `.gitignore` (`bench/results/`)
- `Makefile` (`bench`, `bench-baseline`, `bench-micro`), `.github/workflows/ci.yml` (or `bench.yml`)
- `Dockerfile` (benches are not built in the image; confirm the dependency layer ignores `benches/`)
- `CONTRIBUTING.md`, `docs/ARCHITECTURE.md`

## Verify

- `cargo bench --manifest-path server/Cargo.toml --no-run` and `--manifest-path rust-client/Cargo.toml --no-run` compile.
- `make bench-micro` produces `target/criterion/*/report/index.html`.
- `make bench` runs all four scenarios inside its deadline, writes `bench/results/<sha>.json`, and leaves no server or `cargo run` process behind (`pgrep -f rtdb-server` empty).
- `bun scripts/bench/compare.ts bench/baseline.json bench/results/<sha>.json` exits 0 on an unchanged tree and exits 1 when a result file is hand-edited to +20% p99.
- `make checkall` green (benches are not part of the gate; they must still compile under clippy `--all-targets`).

## Rollback

Benchmarks and scripts are additive; remove the `bench` job and targets. No runtime code changes.
