<instructions>
Each idea has a checkbox that can be marked done.
Work each idea on its own feature branch (see /idea-next).
Commit all work after marking an idea done.
</instructions>

# par-rt-db — Enhancement Ideas

As of 2026-07-28, every ranked Convex-parity gap in `FEATURE_MATRIX.md` is shipped
(all 21 rows ✅). The ideas below reach *beyond* parity — leveraging the project's
self-hosted, Postgres-native, single-writer strengths — and close the handful of
explicitly-deferred sub-items. Effort is rough: `small` ≤1 day, `medium` 1–3 days,
`large` ~a week or more.

## Server & protocol

- [ ] **Webhook / event-delivery registry** — let an app register a URL to be POSTed to on document changes (per table / per event), giving the "no embedded JS / no actions" architecture its native answer for triggering external work. Backed by a side table drained by a worker (or Postgres `LISTEN`/`NOTIFY`), reusing the committer's existing op-feed tap sites so no write path is missed. (effort: large)
- [x] **Prometheus `/metrics` scrape endpoint** — expose the existing gauges/throughput counters as Prometheus text format on an unauthenticated `/metrics` (or admin-gated) route so a self-hosted operator can scrape with Grafana/Prometheus instead of reading only the dashboard JSON. (effort: small)
- [ ] **Per-token and per-database rate limiting / quotas** — the only limiter today is a per-connection WS frame cap (200/10s); add configurable per-machine-token and per-db ceilings so one noisy app on a multi-db instance can't starve the others. (effort: medium)
- [x] **Database deletion + lifecycle management** — `create-db` and `list-dbs` exist but there is no `delete-db`; add an admin endpoint (with a typed confirmation/guard) and a confirmation-driven UI so stale databases can be retired without `psql`. (effort: small)
- [ ] **WebSocket `permessage-deflate` compression** — enable axum/tungstenite's deflate negotiation to cut bandwidth for large result sets and high-frequency pushes. (effort: small)
- [x] **Configurable idempotency-key TTL** — the mutation dedup window is hard-coded to 5 minutes; make it a hot-config value (alongside `max_file_size` etc.) so long-running retry workflows can tune it. (effort: small)

## Query DSL

- [ ] **Aggregations** (`sum` / `avg` / `min` / `max` / `groupBy`) — add aggregate terminals that compile to Postgres aggregate SQL over the same eq-prefix + range-bound WHERE clause every other terminal builds, the same way `count()` already exceeds Convex without an external component. (effort: medium)
- [ ] **Hybrid search (BM25 + vector rerank)** — combine the existing `search` and `vectorSearch` terminals into one ranked-by-both query, the standard high-recall pattern; Postgres can fuse `ts_rank` and cosine `<=>` in a single statement. (effort: medium)
- [ ] **`distinct` terminal** — return unique values (or unique tuples) over an index, mirroring the index-prefix mechanics of the existing terminals; useful for autocomplete/facet UIs. (effort: small)
- [ ] **Batch / multi-query round trip** — accept an array of queries in one HTTP or WS message and return aligned results, cutting client–server round trips for dashboards that fan out over many tables. (effort: medium)
- [ ] **Fine-grained subscription invalidation: range/boundary tracking** — extend today's `get(id)` point-read skipping (FEATURE_MATRIX #21) to ordered/range/set shapes so subscriptions re-run only when a written doc actually falls in their bounds; spec already exists at `docs/superpowers/specs/2026-07-24-fine-grained-subscription-invalidation-design.md`. (effort: large)

## Clients & DX

- [ ] **Python client HTTP / WS / admin / storage surfaces** — close the one explicitly-open client-parity gap: the python client ships wire + DSL only today; port the rust-client's staged rollout (http → reactive ws → admin → storage) so it reaches parity with the TS and Rust SDKs. (effort: large)
- [ ] **`rtdb` CLI** — a terminal tool (schema push, one-shot query/mutate, export/import, token mint) for CI seed scripts and operator workflows that don't want to reach for the dashboard or raw `curl`. (effort: medium)
- [ ] **Per-row authorization model B: collaborator / role fields** — extend the `ownerField` system (FEATURE_MATRIX #20) from single-owner to a declared collaborator/role list, so multi-user apps can share rows without a full predicate DSL. (effort: large)

## Dashboard

- [ ] **Scheduled jobs page** — schedules and cron have a full WS/HTTP surface but no UI; add a page to list, create, pause/resume, and cancel scheduled transactions with their next-fire time and last error. (effort: medium)
- [ ] **Storage / file browser** — file storage has upload/serve/delete/metadata APIs but no dashboard surface; add a per-db file list with size/type, public-URL copy, upload, and delete. (effort: medium)
- [ ] **Query console** — an in-browser scratchpad to compose and run an ad-hoc query or mutation DSL against a chosen database and inspect the raw result, useful for debugging and schema exploration. (effort: medium)
- [ ] **Schema diff / preview on push** — since schema pushes are additive-only, show the operator exactly which columns and indexes a pending push will add (and flag anything rejected) before it is applied. (effort: medium)
- [ ] **Latency percentiles in metrics** — the `/metrics` page tracks gauges, throughput, and (newly) trend sparklines; add p50/p95/p99 latency histograms for query/mutate/subscribe so the dashboard tells a complete performance story. (effort: medium)

## Operations & reliability

- [ ] **Managed `pg_dump` backup scheduling** — the docs reference manual nightly `pg_dump`; add an optional built-in scheduler (configurable cadence + retention to a chosen path) so backups are first-class instead of a cron-and-prayer external step. (effort: medium)
- [ ] **Durable audit log** — the op-feed is ephemeral; add an opt-in durable audit trail (who mutated which table/row, when, from which principal) for multi-user OAuth apps that need accountability. (effort: medium)
