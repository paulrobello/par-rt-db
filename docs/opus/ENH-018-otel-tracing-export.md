# ENH-018 — OpenTelemetry (OTLP) distributed tracing export

> **Source**: kanban card `[ENH-018]`, project `par-rt-db`. Derived from the 2026-08-09 Opus audit.
> **Impact**: medium-high · **Effort**: medium · **Breaking**: no (opt-in, default off)

## Goal

Give operators span-level visibility into where a request's time actually goes — committer queue
wait, SQL execution, subscription fan-out, per-terminal query compilation — by exporting OpenTelemetry
traces over OTLP. Today the server has excellent *aggregate* metrics and no way to answer "why was
*this* mutation slow".

## Current state

- `server/src/metrics.rs` (1,065 lines) is a rich hand-rolled Prometheus surface: counters and
  histograms for mutations, queries, subscriptions, skips per read-set class, quota rejections,
  TTL expiry, missed pushes. Exposed at `GET /metrics` (content-negotiated) and `/admin/metrics` (JSON).
- `server/Cargo.toml:21` has `tracing-subscriber = { version = "0.3", features = ["env-filter"] }`
  and the router carries a `tower-http` `TraceLayer`. **There is no `opentelemetry`, `opentelemetry-otlp`,
  or `tracing-opentelemetry` dependency** — verified by grepping `server/Cargo.toml`.
- So: structured logs go to stdout, aggregates go to Prometheus, and **nothing correlates the two
  across a single request**.

The high-value spans this unlocks are specific to this architecture:

1. **Committer queue wait** — each database has one serialized writer (`committer.rs`). The gap
   between enqueue and execution is invisible today, and it is the single most likely source of
   surprising latency under load. `ARC-102` (idle pollers contending for the 75-connection pool)
   is exactly the class of problem this would have surfaced immediately.
2. **Subscription fan-out** — `subs::fan_out` re-runs affected subscriptions inside the committer
   turn, so a slow subscription query blocks all writes to that database. Per-subscription spans
   make the offender obvious.
3. **Query compile → execute** — `query.rs` compiles the DSL to SQL then executes; splitting those
   two makes "the DSL is slow" vs "Postgres is slow" a distinguishable question.

## Implementation

### Step 1 — Dependencies

Add to `server/Cargo.toml`, all behind a new default-off feature `otel`:

```toml
[features]
default = []
otel = ["dep:opentelemetry", "dep:opentelemetry_sdk", "dep:opentelemetry-otlp", "dep:tracing-opentelemetry"]

[dependencies]
opentelemetry = { version = "0.31", optional = true }
opentelemetry_sdk = { version = "0.31", features = ["rt-tokio"], optional = true }
opentelemetry-otlp = { version = "0.31", features = ["grpc-tonic"], optional = true }
tracing-opentelemetry = { version = "0.32", optional = true }
```

**Verify the exact versions resolve together before committing** — the `opentelemetry` /
`tracing-opentelemetry` version matrix is the usual failure point. Run `cargo update -p opentelemetry`
and check `cargo tree -d` for a duplicated `opentelemetry` tree.

### Step 2 — Boot config

In `server/src/config.rs`, add four boot-only knobs alongside the existing observability settings:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `RTDB_OTEL_ENABLED` | bool | `false` | Master switch |
| `RTDB_OTEL_ENDPOINT` | String | `http://127.0.0.1:4317` | OTLP gRPC collector |
| `RTDB_OTEL_SERVICE_NAME` | String | `par-rt-db` | `service.name` resource attribute |
| `RTDB_OTEL_SAMPLE_RATIO` | f64 | `0.05` | Head sampler ratio |

Use the **new `env_parsed` helper from `QA-106`/`ARC-118`** if that has landed — a malformed ratio
must error at boot, not silently default. If it has not landed, follow the existing idiom and note
the dependency.

**Add all four to `.env.example` AND `docker-compose.yml`'s `environment:` block in the same commit.**
`scripts/env-drift-check.sh` runs first in `make checkall` and will fail otherwise — this is exactly
the gap `DOC2-001`/`DOC2-019` documents.

### Step 3 — Subscriber wiring

In `server/src/main.rs`, where `tracing_subscriber` is initialized, add an OTLP layer when the feature
is compiled in **and** `RTDB_OTEL_ENABLED` is true:

```rust
#[cfg(feature = "otel")]
if config.otel_enabled {
    let tracer = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otel_endpoint)
        .build()?;
    // TracerProvider with a Resource carrying service.name + service.version
    // (reuse the same version/commit strings health.rs:23 already exposes)
    registry = registry.with(tracing_opentelemetry::layer().with_tracer(tracer));
}
```

Install a shutdown hook so the exporter flushes on SIGTERM — a docker `compose down` otherwise drops
the last batch.

### Step 4 — Instrument the load-bearing spans

Add `#[tracing::instrument]` (or manual spans where the arguments are large) at:

| Site | Span name | Key attributes |
|---|---|---|
| `committer.rs` `handle_mutate` | `committer.mutate` | `db`, `steps`, `queue_wait_ms` |
| `committer.rs` `handle_scheduled` / `handle_migrate` / `handle_reaper` | `committer.<arm>` | `db` |
| `subs.rs` `fan_out` | `subs.fan_out` | `db`, `subscriptions`, `skipped`, `reran` |
| `subs.rs` per-subscription re-run | `subs.rerun` | `table`, `terminal`, `read_set_class` |
| `query.rs` `execute_query` | `query.execute` | `db`, `table`, `terminal` |
| `query.rs` compile entry | `query.compile` | `terminal` |
| `txn.rs` `execute_txn` | `txn.execute` | `steps`, `affected` |
| `http_api.rs` handlers | inherited from `TraceLayer` | — |

**Record `queue_wait_ms` explicitly.** Stamp an `Instant` when a `CommitterRequest` is enqueued and
compute the delta when the committer dequeues it. That number is the whole reason to do this work and
it is not derivable from the span tree alone, because the enqueue happens on a different task.

Propagate the incoming `traceparent` header on HTTP so a client's trace continues into the server.

### Step 5 — Documentation

- `deploy/README.md` — new `## Tracing` section: enable the feature, point at a collector, and the
  three spans worth alerting on. Pairs with the `## Monitoring` section `DOC2-020` adds.
- `README.md` — one row in the configuration table.
- `server/README.md` — note the `otel` cargo feature.
- `FEATURE_MATRIX.md` — a row under observability.

## Files to touch

- `server/Cargo.toml` — feature + four optional deps
- `server/src/config.rs` — four knobs
- `server/src/main.rs` — subscriber layer + shutdown flush
- `server/src/committer.rs` — spans on the four arms; enqueue timestamp on `CommitterRequest`
- `server/src/subs.rs` — `fan_out` and per-rerun spans
- `server/src/query.rs`, `server/src/txn.rs` — execute/compile spans
- `.env.example`, `docker-compose.yml` — the four keys
- `deploy/README.md`, `README.md`, `server/README.md`, `FEATURE_MATRIX.md`, `CHANGELOG.md`

**No client mirror required** — this is server-side observability with no wire, DSL, or protocol change.
Say so explicitly in the PR so the four-client rule is visibly considered, not silently skipped.

## Verify

```bash
make -C /Users/probello/Repos/par-rt-db dev-db-up
make -C /Users/probello/Repos/par-rt-db checkall > /tmp/enh018.log 2>&1; echo "EXIT=$?" >> /tmp/enh018.log
grep '^EXIT=' /tmp/enh018.log                                   # must be EXIT=0
make -C /Users/probello/Repos/par-rt-db env-drift-check          # the four new keys are forwarded
cd /Users/probello/Repos/par-rt-db/server && cargo check --features otel
cd /Users/probello/Repos/par-rt-db/server && cargo check          # default build unaffected
```

End-to-end against a collector:

```bash
docker run -d --name otel-test -p 4317:4317 otel/opentelemetry-collector:latest
RTDB_OTEL_ENABLED=true RTDB_OTEL_ENDPOINT=http://127.0.0.1:4317 \
  cargo run --features otel   # from server/
# drive one mutation, then confirm spans arrived:
docker logs otel-test 2>&1 | grep -c 'committer.mutate'          # > 0
docker rm -f otel-test
```

**Acceptance criteria** (mirror these onto the card):
1. `make checkall` green with the feature off (default build byte-compatible in behavior).
2. `cargo check --features otel` compiles clean under `-D warnings`.
3. `make env-drift-check` passes — all four `RTDB_OTEL_*` keys in both `.env.example` and `docker-compose.yml`.
4. With a local collector and `RTDB_OTEL_ENABLED=true`, a single mutation produces a trace containing
   `committer.mutate` with a non-zero `queue_wait_ms` attribute and a child `txn.execute` span.
5. `RTDB_OTEL_ENABLED=false` (default) produces zero OTLP network calls.

## Rollback

Single feature flag plus a boot switch. `RTDB_OTEL_ENABLED=false` disables at runtime with no restart
semantics beyond the usual; building without `--features otel` removes the code entirely. No schema
change, no wire change, no data migration — revert is a plain `git revert`.

## Risks

- **Span cardinality.** Do **not** put `doc_id`, user id, or database *content* on span attributes.
  `db` and `table` are bounded; document ids are not.
- **Committer overhead.** Spans inside the serialized committer turn are on the critical path for every
  write to that database. Keep attribute construction cheap and behind the sampler — measure a
  before/after mutation throughput number and record it in the PR.
- **Version matrix.** The `opentelemetry` / `tracing-opentelemetry` pairing breaks across minor versions
  more often than most crates. Pin exact versions; do not use a floating range.
