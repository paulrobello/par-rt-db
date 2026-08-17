# ENH-024 — Subscription rerun-ratio observability

## Goal

Make the committer-turn subscription fan-out's one residual risk — a database whose
table-level-rerun subscriptions (distinct/aggregate/search/vector) throttle its own write
throughput — observable before it surprises a production workload: a dashboard panel and a
documented alert threshold built on the existing skip/rerun counters.

## Current state

- Audit 2026-08-16 finding ARC-210 (Low, accepted design): re-running affected subscriptions before
  dequeuing the next write is the documented correctness core; the three-class skip-invalidation +
  shadow-verify sampling mitigates it, but rerun-heavy subscription mixes still couple write latency
  to subscriber load.
- Counters exist: `rtdb_subs_skips_total`, `rtdb_subs_reruns_total` (verify exact names in
  `server/src/metrics.rs` / `server/src/subs.rs` before building on them).
- The operator dashboard (`dashboard/`) already has metrics-backed pages (e.g. SlowQueriesPage);
  `/metrics` is deliberately cardinality-bounded (no per-db label on the open endpoint).

## Implementation

1. **Verify the counter surface.** Grep `server/src/metrics.rs` and `subs.rs` for the skip/rerun
   counters; note their label sets. If they carry no per-db dimension, add the ratio to the
   **admin** surface instead of Prometheus labels (respect the cardinality decision): extend the
   existing admin stats endpoint (find it via par-mem `find_api_endpoints` — the dashboard's
   subscriptions page already fetches per-db subscription data) with cumulative
   `subs_reruns`/`subs_skips` per database, sourced from the in-process counters the subs engine
   already keeps (add lightweight per-db atomics next to the Prometheus counters if none exist).
2. **Server**: per-db `AtomicU64` pair on the per-db subs state (incremented where the Prometheus
   counters are incremented — same call sites, no new code paths), exposed through the admin
   stats/subscriptions JSON as `{ "rerunRatio": reruns / max(1, reruns + skips), "reruns": n, "skips": n }`.
3. **Dashboard**: on the existing SubscriptionsPage (or the per-db stats view), render the ratio
   with a warning treatment above 0.5 and a short tooltip explaining what a high ratio means.
4. **Docs**: add a "Monitoring the invalidation fan-out" subsection to `deploy/README.md`:
   the instance-wide PromQL ratio
   `rate(rtdb_subs_reruns_total[5m]) / (rate(rtdb_subs_reruns_total[5m]) + rate(rtdb_subs_skips_total[5m]))`,
   suggested sustained-alert threshold (> 0.5 for 15m), and the remediation levers (narrow the
   subscription, split hot tables, quota caps).
5. **Mirror rule check**: if the admin stats response shape changes, the change is server + admin
   client surfaces — check whether ts/rust/python admin clients type that response and mirror the
   added fields (per the four-way mirror invariant); include them if so.

## Files to touch

- `server/src/subs.rs`, `server/src/metrics.rs` (counter call sites), the admin stats handler
  (locate via `find_api_endpoints`)
- `ts-client`/`rust-client`/`python-client` admin wire types **if** the typed admin response changes
- `dashboard/src/pages/SubscriptionsPage.tsx` (or per-db stats component)
- `deploy/README.md`

## Verification

- `make checkall` green.
- Server test: open a table-level subscription (aggregate), commit writes, assert the admin stats
  response shows `reruns > 0` and a ratio in [0,1]; a skip-classified write moves `skips`.
- Dashboard: component test asserting the warning treatment renders when `rerunRatio > 0.5`.
- Doc: PromQL expression names match the actual metric names (grep-verified).

## Rollback

The per-db atomics and JSON fields are additive; remove the fields and the dashboard panel to
revert. No behavior of the subscription engine itself changes at any point.
