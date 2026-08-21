# Changelog

All notable changes to par-rt-db will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
All packages version in lockstep (see [`docs/RELEASING.md`](docs/RELEASING.md)):
one version for the whole protocol surface, currently `0.1.0`. `swift-client`
carries no manifest version — the repo's release tag is its version.

Feature entries cross-reference the rows in
[`FEATURE_MATRIX.md`](FEATURE_MATRIX.md), which is the authoritative parity
contract against Convex.

## [Unreleased]

### Fix: upsert-insert now stamps the ttl `defaultDurationMs` (server; engines already did)

The server's upsert insert branch applied FM-32 defaults but skipped the
ttl-default stamp, so a document born via upsert on a TTL table silently
carried no expiry — and the server diverged from all four client engines,
whose shared insert paths stamp it. The branch now mirrors `step_insert`'s
exact order (ttl stamp → updatedAt stamp → defaults → owner → authorize).
Upsert-update and replace are unchanged: after insert the field is ordinary
(the ttl design's "never re-stamped after insert" ruling). Pinned by
`server/tests/ttl_test.rs` (upsert-insert stamps within the insert window;
upsert-update leaves the stored expiry untouched) and two semantics-corpus
cases — `upsert-insert-stamps-ttl-default` and
`replace-keeps-supplied-ttl-no-restamp` (which also documents that the spec's
"replace omitting the field stops expiry" scenario is unreachable: a declared
ttl field is required, so such a replace is a `SCHEMA_VIOLATION` on every
runner). Found during the FM-36 client mirrors (two agents independently
flagged it).

### Feature: server-stamped `updatedAtField` (FM-36, server + ts/rust/python/swift)

A table may declare `updatedAtField`, naming a declared `number`/`int64`
field the server stamps with the current epoch-ms on every version-bumping
write — insert, patch, replace, upsert (both branches), `patchByQuery`, and
cascade `setNull` — overwriting any client-supplied value (the `ownerField`
authority model). The value follows the field's wire convention (JSON number
on `number`, decimal string on `int64`), sits in both the doc body and the
typed column when the field is indexed (so "order by recently updated" works
with a declared index), and wins over a `defaults` entry on the same field.
Push-time validation rejects an undeclared, non-numeric, or `ttl.field`-colliding
declaration. Snapshot export/import replays stored docs verbatim — import
never re-stamps. Mirrored in all four client SDKs (schema DSL builder +
in-memory engine stamping at the same seam) with semantics-corpus cases
pinning the wire shape and stamp authority (`updated-at-field-*`), and the
dashboard Schema page (and schema history) show an `updatedAt` badge.

## [0.1.0] - 2026-08-18

First tagged release. Everything below this heading shipped under `[Unreleased]`
before the tag; the dated subsections are chronological within `0.1.0`.

### Fix: grouped aggregate 500s on NULL group keys; engines include the null group (server + ts/rust/python)

`aggregate` with `groupBy` over an optional indexed field returned `INTERNAL`
(a 500) whenever matching rows lacked the group value — the same decode bug
the distinct terminal had (`distinct-includes-null`, one card earlier):
`GROUP BY` includes the SQL NULL group, its key projects to a SQL NULL cell,
and sqlx's `serde_json::Value` decoder rejects NULL cells. The grouped
executor now decodes the key as `Option<Value>` and surfaces the null group
as `key: null`, sorted last (`ORDER BY k`'s NULLS LAST default — the key is
deliberately NOT COALESCEd: jsonb `'null'` sorts before strings, so that
would flip the order). The value cell is now COALESCEd in SQL like the scalar
branch's, so a group whose aggregate input is entirely NULL returns
`value: null` instead of a second 500 (SQL aggregates ignore NULL rows, so a
partially-present group aggregates the present values). The three in-memory
engines mirrored the bug differently — all three silently DROPPED the null
group (on a since-corrected belief that the server's typed column excluded
NULL), and the ts engine additionally fed missing agg values into its reducer
(`sum` over a group with any row missing the field returned `NaN`, and an
all-missing group returned `0`); all three now include the null group sorted
last, skip null agg values, and return `null` for an all-null group. Three
new corpus cases pin it: `aggregate-groupby-includes-null`,
`aggregate-groupby-null-agg-value`, `aggregate-groupby-partial-null-agg`
(all four runners green).

### Fix: distinct terminal 500s on NULL index values (server)

`distinct` over an optional indexed field returned `INTERNAL` (a 500)
whenever matching rows lacked the value: the terminal projects
`SELECT DISTINCT to_jsonb(col) … ORDER BY v`, `to_jsonb(NULL)` is SQL
NULL, and sqlx's `serde_json::Value` decoder rejects NULL cells. The
executor now decodes the projection as `Option<Value>` and surfaces a
missing value as JSON `null` — sorted last, because the unchanged
`ORDER BY v` already places the SQL NULL row last (Postgres ASC defaults
to NULLS LAST, jsonb-type ordering would not: probed live, jsonb `'null'`
sorts BEFORE strings, so a COALESCE-style projection would have flipped
the pinned order). This is the server half of the
`distinct-includes-null` parity gap the SEC-126 entry's engine fixes
exposed — the corpus case's `skip.server` is removed and all four runners
now execute it green.

### Error parity: engines reject kind-mismatched filter values like the server (SEC-126)

The server's filter compile path validates every value-carrying filter leaf
against the field's declared type and returns `BAD_REQUEST` on a JSON-kind
mismatch (`server/src/query/filter.rs` — `field_lhs_and_bind` routes indexed
fields through the `eq_bind_for` typed conversion and declared-but-not-indexed
ones through `validate_jsonb_comparison_value`, the guard that closed
SEC-126's fan-out-forever subscription failure). The three client in-memory
engines evaluated those filters permissively — a number on a string field
matched nothing instead of erroring — so code that passed offline tests could
fail against the real server. All three engines now mirror the server
exactly: indexed fields reuse the engine's eq-bind typing (declared string →
JSON string, number → number, int64 → decimal string, boolean → bool —
preserving the ENH-027 numeric-ordering fix), and non-indexed fields port
`validate_jsonb_comparison_value` including its asymmetry (int64 takes a JSON
number there; decimal strings are rejected). Read terminals, `search`/
`vectorSearch` filters, and the `patchByQuery`/`deleteByQuery` write paths all
validate. Two new corpus cases pin both paths
(`error-filter-kind-mismatch-indexed` / `-jsonb`), and each client gains unit
tests over both paths, `in`-combinator values, and the int64 asymmetry. While
making the new coverage green, all three engines' distinct terminals were
also fixed to include one `null` for absent optional index values (sorted
last) instead of skipping them — the gap `distinct-includes-null` exposed.

### Semantics-corpus coverage: 11 previously unpinned DSL behaviors

Card from the ENH-023 closing review. The corpus (`wire-corpus/semantics/`)
pins the behaviors it covers, but the README documented several DSL behaviors
no fixture exercised — an engine diverging on any of them stayed green. All
are data-only additions; all four runners dir-enumerate the corpus, so no
count assertion needed bumping. Newly pinned: the five txn steps
`replace`/`undelete`/`expectVersion`/`expectAbsent`/`deleteByQuery` (per-step
result shapes — only insert/patch/upsert/patchByQuery were pinned before); the
`or` combinator (every previously pinned combinator was an and/not chain); the
`take`, `first`, and `unique` terminals; `distinct` including NULL index
values; and `aggregate` returning null over an empty match set. Authoring
`distinct-includes-null` surfaced a real server bug — the distinct terminal
500s when the distinct-ed optional index field is missing on some rows
(`to_jsonb(NULL)` is SQL NULL, which sqlx cannot decode into a JSON value;
the aggregate terminal already COALESCEs around exactly this) — so the case
pins the documented semantics for the three client engines and carries a loud
`skip.server` until the one-line fix lands (filed as its own card).

### Dev-db hygiene: `dev-db-clean` autocommit + `sc_` sweep

`make dev-db-clean` rolled back wholesale with "out of shared memory" once
leaked test schemas passed ~2.2k — the script dropped every schema in one
DO-block transaction, and the catalog locks of thousands of cascading DROPs
exceed `max_locks_per_transaction`. The script now generates the DROPs and
executes them through psql `\gexec` in autocommit mode: each schema drop
commits on its own, so progress is durable and an interruption resumes where
it stopped (the script stays idempotent). Its pattern set now also covers the
semantics-corpus runner's `sc_<stem>_<12hex>` databases (schemas +
`rtdb_auth` registry rows) — the runner self-cleans through the same RAII
harness as the integration suites, but its abort-tail had leaked 12 databases
that no cleaner pattern matched. Verified live against 2,324 leaked schemas:
exit 0, zero remaining.

### Property-based parity testing + int64 filter-comparison engine fix (ENH-027)

`server/tests/proptest_parity.rs` generates schemas/documents/queries —
filter trees to depth 3 over the full DSL operator set, int64 boundary
values, optional/null/missing cells — and asserts the Postgres server and the
rust-client in-memory engine agree (ordered comparison when the query's sort
is deterministic, multiset otherwise, system fields projected per the ENH-023
normalization). 64 cases run in the default suite (~4s; `PROPTEST_CASES`
raises the count), and counterexample seeds persist under
`server/proptest-regressions/` so found divergences re-run forever. Its first
generation caught a real divergence, now fixed in **all three** client
engines: an ordering filter (`gt`/`gte`/`lt`/`lte`, and `in`) with a
decimal-string value on a declared `int64` field compared **lexicographically**
in the in-memory engines (`gt("9")` excluded a row with `15`, because
`"15" < "9"` as text) while the server binds typed bigints and compares
numerically. The engines now parse both sides exactly as i64 when the filter
value is a string and the field is a declared `int64` (mirroring the server's
`eq_bind_for(Int64)` path); the minimized case is pinned as
`wire-corpus/semantics/filter-int64-numeric-ordering.json`. CONTRIBUTING
gains the rule: every proptest-found divergence also becomes a
semantics-corpus case so all three clients inherit it.

### Generated, drift-gated CLI reference (ENH-025)

The `cli/README.md` command reference is now generated from the CLI's own
clap definitions (`cli/src/bin/gen-cli-docs.rs` renders the shared
`args::cli()` command at a pinned `term_width`), and `make cli-docs-check` —
wired into `make checkall` — fails when the committed reference no longer
matches the binary, ending the undocumented-subcommand drift class (audit
DOC-202 found 7 of 16 commands undocumented and a false `--url` default).
`make cli-docs` regenerates in place; prose outside the
`cli-reference:begin/end` markers is untouched. The shipped binary now parses
through the same shared command the generator renders.

### Subscription rerun-ratio observability (ENH-024)

The per-db subscription rows served by `GET /admin/subscriptions` and
`GET /admin/metrics` (`perDbSubs`) now also carry the skip-class total and a
`rerunRatio` (`reruns / (reruns + skips)`) computed from the per-db counters
ENH-010 already kept — making a rerun-heavy subscription mix (table-level
reruns of distinct/aggregate/search/vector subscriptions coupling write
latency to subscriber load) visible before it surprises a production
workload. The dashboard Subscriptions page renders the ratio with a warning
treatment above 0.5, and `deploy/README.md`'s "Monitoring the invalidation
fan-out" gives the instance-wide PromQL ratio, a sustained >0.5-for-15m alert
threshold, and the remediation levers (narrow the subscription, split hot
tables, quota caps). `DbSubCounters` is mirrored across the ts, rust, and
python clients.

### Security: non-zero per-IP rate-limit defaults (SEC-203)

Code defaults change from 0 (off) to: `RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM`
0→10, `RTDB_ANONYMOUS_RATE_LIMIT_PER_IP_RPM` 0→10,
`RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM` 0→300. Explicit `0` still disables each.
Behavior change only for bare-env deployments that set no env vars — the
shipped `docker-compose.yml` already set 10/10/600 (compose keeps its
deliberately looser 600 for gallery-heavy traffic; `.env.example` now ships
300). `RTDB_RATE_LIMIT_PER_TOKEN_RPM` / `RTDB_RATE_LIMIT_PER_DB_RPM`
deliberately stay 0: they bound authenticated traffic, where a surprise
non-zero default can break real apps — opt in per deploy.

### Security: forwarding headers gated on `RTDB_TRUSTED_PROXY` (SEC-201)

`CF-Connecting-IP` / `X-Forwarded-For` (per-IP rate-limit keys on the public
storage, admin-login, and anonymous-auth routes) and `X-Forwarded-Proto`
(cookie `Secure` attribute, HSTS) are now consulted only when the new
`RTDB_TRUSTED_PROXY` env var is `true`. Default `false`: on a directly
reachable port those headers are caller-controlled, and trusting them let an
attacker mint a fresh rate-limit bucket per request. The shipped
`docker-compose.yml` (behind the Cloudflare tunnel) sets it to `true`, so the
deployed behavior is unchanged; bare-env deployments that DO sit behind a
tunnel/proxy must set `RTDB_TRUSTED_PROXY=true` or rate limiting will key on
the proxy's address instead of the client's.

### Phrase/operator search + snippets (FM-31)

Search query text is parsed by `websearch_to_tsquery` (was `plainto_tsquery`):
quoted phrases (`"database notes"`) require adjacency, a bare `or` unions
alternatives, `-term` excludes; plain multi-term queries keep AND semantics
(pinned equivalent by tests). The query text stays `$n`-bound with the index's
regconfig, so the upgrade is injection-safe. An optional additive
`snippet: true` on the `search` terminal attaches a `_searchSnippet` string to
every hit — a `ts_headline` fragment with matched terms wrapped in
`<mark>…</mark>` and server-fixed word bounds (`MaxWords=35`, `MinWords=15`);
the client supplies only the boolean. `snippet` is tsquery-mode only:
`snippet: true` + `mode: "trgm"` is a `BAD_REQUEST` (trgm matches substrings —
there is no tsquery tree to highlight). Mirrored in all four clients
(`.search()` opts `snippet`) including the three in-memory harnesses
(adjacent-phrase contains, or-union, minus-exclusion, `<mark>` excerpt stub).
Spec: `docs/superpowers/specs/2026-08-15-phrase-search-snippets-design.md`.

### Substring/autocomplete search via pg_trgm (FM-30)

The `search` terminal accepts an optional `mode: "tsquery"|"trgm"` (omitted =
today's full-text behavior, byte-identical). `trgm` runs case-insensitive
`ILIKE '%q%'` over the search index's text fields — prefix, infix, and
autocomplete fragments that lexeme-based tsquery cannot match — ranked by
`GREATEST(similarity(field, q))` across fields (tie-break `created_at`/`id`
desc), composing with `filter` and `take`. Backed by the `pg_trgm` extension
(auto-created like pgvector) plus a GIN trigram index (`tg_<table>_<index>`)
beside each search index's tsvector GIN: `CREATE INDEX IF NOT EXISTS` on every
additive schema push, so pre-existing deployments backfill on the next push
(idempotent); destructive reconcile and migrate `dropIndex` drop both GINs.
Tradeoff: roughly double the index storage over search fields. Mirrored in all
four clients (`.search()` opts `mode`) including the three in-memory harnesses.
Spec: `docs/superpowers/specs/2026-08-15-trgm-search-design.md`.

### Anon→real account merge (FM-27)

An anonymous user who later signs in with OAuth is merged into the real
account. `GET /auth/{provider}/begin` resolves the caller's anon session
server-side (never caller-supplied) and records it on the pending login row
(`rtdb_auth.oauth_states.anon_user_id`); the callback then merges
synchronously via `merge::merge_users`, crash-safe by ordering: per-database
principal-bearing doc restamps each inside that db's committer turn
(`CommitterRequest::RunMergeUsers`, publishing through the op-feed/audit/
webhook taps with `source = "merge"`), storage blob owner swap, session
re-point (an open WS or stored SDK token promotes to the real principal on
its next op), then a guarded anon-row delete. Every step is idempotent; any
interruption is recovered by signing in again. `POST /admin/merge-users`
(typed confirm, 404 on a missing anon row) runs the merge synchronously as
the operator escape hatch; `rtdb_merge_docs_total` counts restamped docs.
Spec: `docs/superpowers/specs/2026-08-14-anon-merge-design.md`.

### Cross-replica OAuth login state (ENH-022, Stage 0 + Stage 1)

The single-use OAuth `state` token minted at `/auth/{provider}/begin` and
consumed at `/auth/callback` now lives in a `rtdb_auth.oauth_states` table
instead of an in-process `HashMap`. The lifecycle (`pending` → `claiming` →
`completed` | `failed`) is enforced by the database: `claim_pending` does a
conditional `UPDATE ... WHERE status = 'pending'` (single-use claim, replay →
400, race-safe across replicas), and `poll_login` consumes a terminal row with
`UPDATE ... WHERE consumed_at IS NULL RETURNING` so a second `/auth/state` poll
for the same token delivers nothing. This **fixes the silent 2-replica OAuth
login break** — a login begun on one replica completes on another — and closes
the `SEC-132` note that the in-memory map was pruned only opportunistically. A
gated 60 s background sweep deletes expired rows (no new ungated poller).

This is Stage 1 of ENH-022; it does not by itself make multi-instance supported.
Stage 0 documents the constraint that remains: a boot `WARN`, a "Known MVP
limitations" entry in `README.md`, and a "Topology: single instance" section in
`deploy/README.md` naming all four in-process state pieces (op-feed, presence,
rate limiting remain instance-local; the single-writer committer invariant is
untouched). Cross-instance op-feed, presence gossip, shared rate limiting, and
the writer-funnelling decision are later stages. No wire/protocol/DSL change —
no client mirror required.

### Typed backfill expression grammar — `evalExpr` closes SEC-107 (ENH-020)

The `evalExpr` migrate directive's backfill expression is now a **closed, typed
`ValueExpr` grammar** instead of raw SQL. `Field`/`Literal`/`Concat`/arithmetic/
`Coalesce`/`Lower`/`Upper`/`Trim`/`Cast`/`Now`/`Case` — every literal is bound
as a `$n` parameter, every field is schema-validated against the table's
`TableDef`, and there is no subquery, function-call-by-name, or raw-SQL node.
The SEC-107 injection concern (a newline before `FROM` and a bare `SELECT`
without `FROM` both bypassed the old denylist) cannot arise from a `ValueExpr`
payload by construction.

**Dual-accept rollout**: the legacy raw-SQL string form is still accepted for one
deprecation cycle, but gated to the root `admin_key` only. The typed path is
available to delegated dashboard admins for safe backfills. A typed `expr` mixed
with a legacy `where` (or vice versa) is rejected.

**Wire-breaking** for clients emitting the legacy string form that later want
the safe path: migrate to `ValueExpr` before the string form is removed.
`ValueExpr` is mirrored byte-identically across all four clients (ts-client
`ValueExprJson`, rust-client `ValueExpr`, python-client `ValueExpr`) with
`evalExprTyped`/`eval_expr_typed` builder ergonomics, plus wire-corpus cases.

### Streaming storage upload/download (ENH-021)

File size is now decoupled from server RAM. Uploads and downloads stream
through the server in 1 MiB chunks instead of buffering the whole blob, so
N concurrent uploads of a large file no longer cost N × filesize of resident
memory, and `RTDB_MAX_FILE_SIZE` becomes a policy limit rather than a
memory-safety limit. No wire, protocol, or route change — same `POST
/api/storage/{db}` raw-body contract; the HTTP surface is unchanged.

- **Chunked layout**: a new per-db `storage_chunks(blob_id, seq, bytes)` table
  holds new blobs at 1 MiB chunks; the existing `storage` row stays as the
  metadata record (id/sha256/size/contentType/createdAt/owner_id) with its
  inline `bytes` now nullable. Created in `create_database` and retrofitted
  idempotently by `storage::ensure_table`, so pre-ENH-021 databases upgrade
  transparently. Legacy inline blobs still serve (full and ranged) via a
  lazy `probe_layout` fallback — never eager-migrated at boot (the ARC-102
  unbounded-work anti-pattern).
- **Streaming upload**: `upload_handler` consumes `into_data_stream()`
  instead of `to_bytes`, enforcing `RTDB_MAX_FILE_SIZE` incrementally
  (aborts the moment the running total exceeds the limit, rather than after
  buffering) and the per-db storage quota mid-stream (`QUOTA_EXCEEDED`/507,
  committing nothing). Dedup (ENH-008) is preserved by writing chunks under
  a provisional id, computing the final sha256 during the stream, and on a
  content hit deleting the provisional chunks and returning the existing id.
  `owner_id` (SEC-118) stamping is unchanged.
- **Streaming download + chunk-aware Range**: serve builds an axum body from
  a stream over `storage_chunks ORDER BY seq`; a `Range` request reads only
  the covering chunk span, byte-trimming the first and last. The 206 /
  `Content-Range` / 416 semantics are unchanged. Image transforms fetch all
  chunks (the decoder needs the full bytes) and remain cache-keyed as whole
  renders.
- **Raised ceiling**: the compile-time `HARD_MAX_FILE_SIZE` clamp rose
  50 MiB → 2 GiB (now a disk/quota guard, since upload no longer buffers).
  The admin-mutable `RTDB_MAX_FILE_SIZE` default stays 50 MiB; a
  compromised admin token still cannot raise the compile-time clamp.
- **Client mirrors**: `ts-client` `upload` accepts
  `Uint8Array | Blob | ReadableStream | ArrayBuffer | string` (passes the
  body to `fetch` verbatim, no buffering); `rust-client` adds
  `upload_stream<S: TryStream>` (`reqwest::Body::wrap_stream`); `python-client`
  `upload` accepts `bytes | IO[bytes] | Iterable[bytes]` (sync) and
  additionally `AsyncIterable[bytes]` (async). All keep their existing
  buffer-accepting overloads.

### OpenTelemetry (OTLP) distributed tracing export (ENH-018)

Span-level visibility into where a request's time goes — committer queue wait,
SQL execution, subscription fan-out, per-terminal query compilation — exported
over OTLP/gRPC to a collector. Opt-in: a new `otel` cargo feature (default off)
gates the dependencies and subscriber wiring, and `RTDB_OTEL_ENABLED=false`
(the default) gates it again at runtime, so a feature-compiled binary still
makes zero OTLP network calls unless an operator opts in. Server-side only —
no wire, DSL, protocol, or client-mirror change.

- **Spans**: `committer.mutate` (carries `queue_wait_ms` — the gap between
  enqueue and dequeue, the single most useful per-request latency signal and
  the one the architecture made invisible), plus `committer.subscribe` /
  `scheduled` / `migrate` / `reaper` / `restore_schema`, `subs.fan_out`,
  `subs.rerun` (per subscription re-run), `query.execute`, and `txn.execute`.
  Child spans nest under their parent (e.g. a mutate's `txn.execute` and
  `subs.fan_out` are children of `committer.mutate`).
- **Cardinality guard**: span attributes are bounded — `db`, `table`,
  `terminal`, `steps`, `queue_wait_ms` — never doc ids, user ids, or document
  content.
- **Config**: `RTDB_OTEL_ENABLED`, `RTDB_OTEL_ENDPOINT` (default
  `http://127.0.0.1:4317`, the standard OTLP/gRPC port), `RTDB_OTEL_SERVICE_NAME`
  (default `par-rt-db`), `RTDB_OTEL_SAMPLE_RATIO` (default 0.05, clamped to
  `[0, 1]`; a malformed value fails boot via `env_parsed` rather than silently
  defaulting — ARC-118).
- **Shutdown**: the OTLP exporter flushes on SIGTERM (a docker `compose down`
  otherwise drops the last in-flight batch).
- **Version matrix**: `opentelemetry`/`opentelemetry_sdk`/`opentelemetry-otlp`
  0.31 + `tracing-opentelemetry` 0.32, pinned because that pairing breaks
  across minor versions. The 0.31 API renames `TracerProvider` →
  `SdkTracerProvider`, drops the runtime arg from `with_batch_exporter`, and
  replaces `Resource::new` with a builder.

Verified end-to-end against a local `otel/opentelemetry-collector`: a mutation
produces a trace with `committer.mutate` (`queue_wait_ms: 5` on a queued
request) and child `txn.execute` + `subs.fan_out` spans; with the default
off, zero traces are exported.

### Query introspection: explain + slow-query log (ENH-019)

Two admin endpoints that make the query layer observable without running a
query: one compiles a plan, the other replays the recent past. Both are
admin-only and server-side; the explain path returns no rows.

- **`POST /admin/db/{db}/explain`** — re-compiles a Query JSON DSL through the
  same `compile_query` the real query path uses, returning
  `{sql, params, terminal, warnings}`: the exact parameterized SQL Postgres
  would execute, bind values formatted as strings (numbers/booleans via
  `Display`), the query terminal kind (`get`/`collect`/`count`/`unique`/`first`/
  `distinct`/`aggregate`/`paginate`/`search`/`vectorSearch`/`hybridSearch`),
  and compile-time warnings (currently unindexed-filter — a filter on a field
  with no backing index, the most common cause of a slow `collect`).
- **`GET /admin/slow-queries`** — bounded in-memory ring (`VecDeque`, no
  Postgres) of queries whose wall-clock duration exceeded `RTDB_SLOW_QUERY_MS`.
  Each entry carries `startedAtMs`, `durationMs`, `db`, `table`, `terminal`,
  the exact `sql` string explain emits (so a slow row is reproducible via
  EXPLAIN), and `params` only when `RTDB_SLOW_QUERY_LOG_PARAMS=true` — the
  default `false` keeps document content out of the admin log until an
  operator opts in. The response includes `thresholdMs` (0 when logging is
  disabled) and `capacity`.
- **Config**: `RTDB_SLOW_QUERY_MS` (default 0 = disabled), `RTDB_SLOW_QUERY_CAPACITY`
  (default 200; the response never returns more than this many rows),
  `RTDB_SLOW_QUERY_LOG_PARAMS` (default false).
- **Clients**: `RtDbAdminClient.explainQuery(db, query)` /
  `getSlowQueries(opts?: { db?, limit? })` in the ts-client; dashboard Slow
  queries page wires both (the page's inline explain panel accepts a Query
  JSON DSL and renders the returned SQL, bind values, terminal, and warnings).

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

### Shipped 2026-07-26 → 2026-08-07

Post-audit feature work, all under `[Unreleased]` (no tagged release yet). Each
entry cross-references the FEATURE_MATRIX row that is the authoritative parity
contract.

#### Server

- **Microsoft + Apple OAuth** (FEATURE_MATRIX #14) — `auth/microsoft.rs` (Entra ID/Azure AD v2; derives endpoints from `RTDB_MICROSOFT_TENANT`) and `auth/apple.rs` (ES256 JWT `client_secret` signed per-exchange with the registered EC key, `response_mode=form_post` served by a dedicated POST `/auth/apple/callback`, identity keyed on Apple's stable `sub` via a new `apple_sub` column). Six providers now ship behind the `OAuthProvider` trait.
- **Login-CSRF defense** (SEC-012) — `rtdb-oauth-csrf` double-submit cookie (`SameSite=None;HttpOnly`) set at `GET /auth/{provider}/begin`, constant-time-verified at the callback; kill-switch `RTDB_OAUTH_LOGIN_CSRF=false`. The OAuth popup opens `noopener,noreferrer` (reverse-tabnabbing); completion is relayed by the parent polling `GET /auth/state?state=<token>` (not `window.opener.postMessage`, which `noopener` severs). The state token, not the cookie, is the poll capability — the flow works cross-origin where the `SameSite=Lax` session cookie would not be sent.
- **Schema migration** (`POST /admin/db/{db}/migrate`) — ordered `Directive` list (`renameField`/`renameTable`, `changeType` with closed cast matrix + optional `default`, `dropField`/`dropTable`/`dropIndex`, `setDefault`, scoped `evalExpr` raw-SQL doc-rewrite escape) running inside the committer's serialized `RunMigrate` arm so `fan_out` + op-feed + audit + webhook all fire. Dry-run-first. Mirrored across all four clients + the `rtdb migrate` CLI + the dashboard (preview → review → apply).
- **Schema change history** (ENH-013) — every `push-schema`/`migrate`/`restore` captures a snapshot; `GET /admin/db/{db}/schema/history[?limit=&offset=]`, `GET /admin/db/{db}/schema/history/{version}`, `POST /admin/db/{db}/schema/restore` (in-place destructive shape reconcile inside the committer, captures the outgoing shape first so the restore itself is undoable).
- **Per-row auth Model B + Model C** (FEATURE_MATRIX #20) — `collaboratorsField` (owner OR collaborator) and `authorize` (a general `FilterExpr` predicate over doc fields plus `$user`/`$email` principal markers; pre-check + auto-stamp + post-write verify on all five write paths). `ExpectVersion`/`ExpectAbsent` side-channel closed (2026-08-03): a doc the caller cannot see is indistinguishable from absent.
- **Unique + partial (`WHERE`) indexes** (FEATURE_MATRIX #22) — `unique` + `where: FilterExpr` on `IndexDef`; partial predicate compiled to literal SQL at DDL time; `unique_violation` → `CONFLICT` (HTTP 409) wire code.
- **Document TTL / auto-expiry** (FEATURE_MATRIX #23) — `ttl: { field, defaultDurationMs? }` declaration; per-db reaper task enqueues `RunReaper` every `RTDB_TTL_SWEEP_INTERVAL_SECS`, batch-deletes expired rows inside the committer's serialized turn, publishes through all four tap sites with `source = "ttl"`, `owner = None`.
- **Webhooks** (ENH-003, when `RTDB_WEBHOOKS_ENABLED=true`) — per-`DocOp` outbox row drained by a boot worker (reqwest POSTs, exponential backoff, at-least-once); admin CRUD at `/admin/db/{db}/webhooks`.
- **Audit log** (ENH-004, when `RTDB_AUDIT_LOG_ENABLED=true`) — best-effort `rtdb.audit_log` row per `DocOp` at the committer tap sites (`ts_ms, db, table, op, doc_id, principal, source`); `GET /admin/audit?db=&limit=&offset=`.
- **Scoped machine tokens** (ENH-005) — optional `expiresAt`/`readOnly`/`tables` scoping with live expiry, mirrored across ts/rust/python clients + dashboard.
- **Subscription inspector** (ENH-010) — `GET /admin/subscriptions` lists active subscriptions across all dbs with read-set class + skip/re-run counters.
- **Database clone + delete** (ENH-009) — `POST /admin/clone-db` (schema-only clone); `POST /admin/delete-db` (typed `{name, confirm}` guard, retires the per-db committer/scheduler/reaper tasks cleanly).
- **Search language config** (ENH-006) — `RTDB_SEARCH_LANGUAGE` boot config (Postgres `regconfig` for the generated tsvector + `plainto_tsquery`).
- **Vector distance metrics** (ENH-007) — per-vector-index `metric: cosine | l2 | ip` (cosine default); HNSW index over `vector_cosine_ops`/`vector_l2_ops`/`vector_ip_ops`, ranking distance `<=>`/`<->`/`<#>`.
- **Storage dedup** (ENH-008) — `sha256`-keyed dedup at upload; a second upload of the same bytes returns the existing id.
- **Image transforms** (ENH-014) — pure-Rust decode → resize → re-encode on both serve routes (`?w=&h=&fit=&q=&format=`); in-memory `moka` cache, bounded decode concurrency, decode-pixel cap. Passthrough is zero-overhead; transformed responses carry `Cache-Control: public, max-age=31536000, immutable`. `RTDB_IMAGE_*` knobs.
- **Realtime presence** (FEATURE_MATRIX #25, ENH-015) — transient, in-memory, connection-bound (the open `/sync` WS is the liveness signal), not committer-bound or persisted, coalesced via a process-wide flush task. Per-state TTL follow-up: `presenceState` accepts `ttlMs`; the server clears state to `null` after the TTL while the member stays. `RTDB_PRESENCE_*` knobs; `RTDB_PRESENCE_ENABLED` master switch.
- **Per-database resource quotas** (FEATURE_MATRIX #26, ENH-011) — three global caps on `HotConfig` (`maxTablesPerDb`, `maxStorageBytesPerDb`, `maxSubsPerDb`, all `0` = unlimited) enforced hard at push-schema/migrate, `handle_subscribe`, and `handle_mutate`/`handle_scheduled`/`handle_migrate`/`upload_handler`; cached live `pg_total_relation_size` measurement; **no admin bypass** (PrincipalCtx can't distinguish admin from machine token at the committer). Over-cap → `QUOTA_EXCEEDED` (HTTP 507); `rtdb_quota_rejections_total{kind}` metric. Mirrored across all four clients.
- **Backup lifecycle** — `POST /admin/backup` (manual `pg_dump`, 409 if already running), `GET /admin/backups`, `GET|DELETE /admin/backups/{name}`, `POST /admin/restore` (restores into a fresh `rtdb_restored_<stamp>` DB via `pg_restore --no-owner --no-privileges`; the live `rtdb` DB is never touched). Requires `CREATEDB` on the DB role. Mirrored in ts/rust/python admin clients + dashboard.
- **Async python client** (ENH-012) — `RtDbAsyncHttpClient` over `httpx.AsyncClient` (`pip install par-rt-db[aio]`); same method set as the sync twin.

#### Clients (cross-cutting)

- **All four clients at feature parity** (2026-07-29) — ts/rust/python + dashboard. Optimistic updates, in-memory/offline test harness, the full admin control plane (allowlist/admins/metrics/hot-config/ops-feed/tokens/schema/stats), and the wire/DSL surfaces now mirror across all four ports.

### Shipped 2026-08-08 → 2026-08-10

Continued post-audit feature work, still under `[Unreleased]` (no tagged
release). Anchored by the 2026-08-10 audit-remediation cycle and the features
that landed in the same window; entries cross-reference the FEATURE_MATRIX row
that is the authoritative parity contract.

#### Server

- **Active-session management** — `GET /admin/sessions?user=&limit=&offset=` lists live sessions; `DELETE /admin/sessions?user=` revokes every session for a user; `DELETE /admin/sessions/{token_hash}` revokes one. Revocation is live per-op: an open `/sync` connection's next Subscribe/Mutate re-runs the session check and closes on a revoked token. Mirrored across ts/rust/python clients + the `rtdb` CLI.
- **Anonymous auth** (`POST /auth/anonymous`, gated `RTDB_AUTH_ANONYMOUS_ENABLED`, default off) — mints an ephemeral `rtdb_auth.users` row (`anonymous = TRUE`, no email) plus a session, returning the session token in the body and setting the same HttpOnly cookie as OAuth. An anonymous user is a `Principal::User` with `anonymous = true`, `email = None` — `authorize` bypasses the per-db allowlist for it (its creation is its authorization), and per-row `ownerField` stamps its `user_id` so it owns its own drafts/cursors. The anon→real merge on a later OAuth sign-in shipped 2026-08-14 — see the FM-27 entry above. ts-client exposes it as `useRtDbAuth().signInAnonymous()`; machine-side clients (rust/python) are out of scope. See FEATURE_MATRIX #20.
- **By-query transaction steps** — `PatchByQuery{table, filter, patch, limit?}` and `DeleteByQuery{table, filter, limit?}` find rows matching the same `FilterExpr` `.filter()` accepts and act on them in one serialized committer turn. Row visibility matches the read path exactly (the caller's interactive filter composes with `ownerField`/`collaboratorsField`/`authorize`); each affected row records a `DocOp`/`WriteSet` entry so subscriptions, op-feed, audit, and webhooks fire per row. `MAX_BY_QUERY_ROWS = 1000` bounds rows per step; `MAX_BY_QUERY_STEPS_PER_TXN = 16` bounds by-query step count per transaction.
- **`MAX_STEPS` 256 → 1024 + per-transaction affected-row budget (SEC-104)** — the per-transaction step cap is raised to 1024 (ARC-104), and a new aggregate budget `MAX_AFFECTED_ROWS_PER_TXN = 10000` bounds a transaction's worst-case row count (per-id steps count one document each; each by-query step counts up to its `limit`). An over-budget transaction is rejected **before** the committer — an over-cap write never becomes durable. Mirrored across all four clients (wire-corpus pinned).
- **`count` aggregate** — the `aggregate` terminal now supports `sum`/`avg`/`min`/`max`/`count`. `count` counts rows and consumes no aggregate field; grouped `count` is the count-per-group the dashboards need.
- **`filter()` on `search` and `vectorSearch`** (#11, #17) — both query terminals now accept an optional full `FilterExpr` (the same predicate `.filter()` accepts — `vectorSearch` was widened from the original eq-only map over `filterFields` in commit `613c7a6`); ranked results are post-filtered server-side. The `FilterExpr` predicate itself gained `not`/`contains`/`exists` variants.
- **HTTP `Range` requests on storage** (ENH-016) — both serve routes (`GET /storage/{id}`, `GET /api/storage/{db}/{id}`) honor single-range `Range: bytes=…` requests with `206 Partial Content` + `Content-Range`/`Content-Length`/`Accept-Ranges`; out-of-bounds → `416 Range Not Satisfiable`; multipart/non-`bytes`/malformed ranges are ignored per RFC 7233. Read-path only — no committer, protocol, or WS change. Image-transform responses are whole renders and skip `Range`.
- **Signed, time-limited storage URLs** (ENH-017) — `GET /api/storage/{db}/{id}/signed-url?ttlSeconds=3600` (bearer-authorized for `{db}`) mints a URL granting read access to one blob until an absolute expiry (default 1h, max 7d): `GET /storage/{id}?exp=<unix-ms>&sig=<hex>`, verified by an HMAC key derived from `admin_key`. A request with no `exp`/`sig` still serves publicly as before; a bad signature returns 403 `FORBIDDEN`. The `{db}` is in the minting route (SEC-113 — cross-db returns 404) because raw bodies can't carry it and session principals aren't db-scoped.
- **`jsonwebtoken` 9 → 10.4.0** (CVE-2026-25537) — bumped the JWT crate and switched to the `RustCrypto` signing/verification backend (commit `dc9e958`). The 9.x line carried a critical verification vulnerability; 10.4.0 on the RustCrypto backend resolves it.

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
- **Fine-grained subscription invalidation** — `get(id)` point reads skip re-runs when the write didn't touch their document; `count`/`collect`/`unique` on a btree index's eq-prefix (+ optional range bound) skip when every written doc is provably outside their window (`WriteSet.doc_values` carries before/after; never under-approximates — deletes always re-run); `take(N)`/`first`/`paginate` skip when every written doc is outside that window *or* ranks beyond the last result's final row (the top-N boundary, refreshed on every re-run; an unfull result is unbounded), for which `doc_values` also carries each written doc's `created_at`; `distinct`/`aggregate`/`search`/`vector`/`hybrid` stay table-level (#21). Guarded by two safety nets, since a wrong skip is otherwise silent: `cmp_binds` is structured so a new `EqBind` variant is a compile error rather than an under-approximating fallback, and `RTDB_SUBS_VERIFY_SKIP_EVERY` shadow-verifies 1 skip in every N — a divergence logs at ERROR, increments `rtdb_subs_missed_pushes_total`, and pushes the corrected result. (This entry described the verifier with a default of 0 = off when written; the shipped default was later raised to 1000, so it is on unless explicitly set to 0.) Skip/re-run effectiveness is counted per read-set class (`rtdb_subs_skips_total{class}`) on `/admin/metrics` + `/metrics`, mirrored in the ts and rust clients, and shown on the dashboard metrics page.
- **Operator dashboard backend** — admin allowlist CRUD (`/admin/admins`), per-db metadata (`/admin/dbs/{db}/{schema,stats}`), live metrics (`/admin/metrics`), realtime op feed (`/admin/ops/recent`, `WS /admin/stream`), hot-reloadable config (`GET/PATCH /admin/config`), admin document access (`POST /admin/db/{db}/query|mutate`, `RTDB_MAX_AFFECTED_DOCS` cap), and same-origin static SPA hosting gated on `RTDB_STATIC_DIR` (#18).
- **Extra validators** — `record`, `any`, `bytes`, `int64` (JSON-string of decimal digits, branded `Int64` on the TS client) (#13).
- **OAuth admin allowlist seed** — `RTDB_ADMIN_EMAILS` env seeds `rtdb_auth.admins` at boot.

#### TypeScript client (`@par-rt-db/client`, `ts-client/`)

- **No-codegen schema** — TS source of types; `Doc`/`Id` inferred from the schema.
- **Reactive WebSocket client** — auto-reconnect, re-auth, resubscribe, heartbeat, stale-callback generation guard.
- **React bindings** — `RtDbProvider`, `useQuery`, `useMutation`, `useConnectionState`, auth gates, `usePaginatedQuery`.
- **HTTP/admin clients** — one-shot query/mutate, schedule, storage, token mint/revoke, schema push, snapshot export/import.
- **In-memory test harness** — `InMemoryRtDbClient` mirroring server query/txn/subscription semantics offline.
- **Optimistic updates** — opt-in `optimisticUpdates` overlaid on each subscription's last result; server reconciles, rolls back on error (#12). Mirrored in the rust and python clients.

#### Rust client (`par-rt-db-client`, `rust-client/`)

- Wire contract, schema/mutation/query DSL, http + reactive ws + admin clients, index helpers, `mutate_with_retry`, `.filter()`/`.search()`/`.vector_search()` builders, schedule + storage surfaces.
- Opt-in live-server integration test (`tests/http_integration.rs`, `#[ignore]`, `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`).
- Optimistic updates (matches FEATURE_MATRIX #12).

#### Python client (`par-rt-db`, `python-client/`)

- Wire contract — `ServerMessage` / `ClientMessage` unions, `AuthedUser`, `Schedule*`, `FilterExpr`, `SearchQuery`/`VectorSearchQuery`, `RtDbError`/`ErrorCode`.
- Schema DSL — 15 `FieldType` variants, btree/search/vector indexes, `TableBuilder`, `ownerField`.
- Mutation DSL — `Step`/`StepResult`/`Transaction`/`Mutation` builders.
- Query DSL — `TableQuery` builders (`get`/`with_index`/`eq`/`gt`/…/`order`/`take`/`unique`/`first`/`count`/`filter`/`search`/`vector_search`/`paginate`), `Query`/`QueryResult`, `encode_cursor`/`decode_cursor`.
- Four-way wire-parity fixtures (Python ↔ server ↔ TS ↔ Rust).
- HTTP/WS/admin/storage client surfaces shipped: sync `httpx` client (`pip install par-rt-db[http]`), async twin over `httpx.AsyncClient` (`pip install par-rt-db[aio]`, ENH-012), reactive `RtDbClient` over `/sync` (`pip install par-rt-db[ws]`), admin client + storage helpers, optimistic updates, and an in-memory/offline test harness (`par_rt_db.in_memory` + `tick()`). The four clients are now at feature parity (2026-07-29).

#### Dashboard (`@par-rt-db/dashboard`, `dashboard/`)

- Operator console SPA — Vite + React 19 + TS, served same-origin at `RTDB_STATIC_DIR`.
- Three-pane "Instrument Manual" UI — admin-key + OAuth (GitHub/Google) login; databases index + per-db stats; schema spec sheet; live data browser (realtime over `/sync` for OAuth admins, ~2s polling for admin-key mode); live metrics instrument panel; op-feed page; hot-config editor; admin allowlist CRUD. Op feed + metrics stream over a single WS to `/admin/stream` (subprotocol auth).

#### Build / operations

- Root `Makefile` with `make build | fmt | fmt-check | lint | typecheck | test | checkall | dev-db-up | dev-db-down | pre-commit | pre-commit-update | deploy` spanning all six packages (server, ts-client, rust-client, python-client, dashboard, cli).
- `pre-commit` runs `gitleaks` + `detect-private-key` + format/lint checks.
- Docker deploy (`Dockerfile`, `docker-compose.yml`) — the dashboard SPA is baked into the image (`dashboard` build stage copies `dist/` to `/app/dashboard-dist`, pointed at by `RTDB_STATIC_DIR`).
- Healthz `/healthz` — `{status:"ok"|"degraded", version, git_commit, build_timestamp, started_at, uptime_seconds, postgres}`; `RTDB_BUILD_COMMIT` bake-in via build-arg for image builds without `.git`.
- Graceful shutdown on `SIGINT`/`SIGTERM` (waits for in-flight requests + open WebSockets; Docker SIGKILL is the backstop).

### Changed

- **Hot config** (`allowed_origins`, `session_ttl_days`, `max_file_size`) — runtime-mutable via `PATCH /admin/config`; persisted in `rtdb_config`, swapped live via `Arc<ArcSwap<HotConfig>>` (no restart). The CORS layer re-reads `allowed_origins` per request.
- **`GET /admin/config`** is structurally redacted — `admin_key`, OAuth secrets, and `database_url` are exposed as configured-bools only, never values.
- **C collation** — Postgres database uses deterministic C collation, eliminating collation-version warnings and making index ordering deterministic.
- **OAuth callback HTML** — strict origin validator + interpolation escaping (security hardening).
- **rust-client: `RtDbAdminClient` extracted** (ARC-121) — the admin control-plane methods (`/admin/*`) moved off `RtDbHttpClient` into a dedicated [`RtDbAdminClient`](rust-client/src/admin.rs) type, mirroring `ts-client`'s and `python`'s split between data plane and control plane. **Non-breaking:** every admin method remains on `RtDbHttpClient` as a `#[deprecated(note = "use RtDbAdminClient")]` thin delegation to the new type, so existing consumers (including the `rtdb` CLI, which migrated to `RtDbAdminClient` directly) keep compiling. Migrate by calling `RtDbHttpClient::admin_client()` (shares the connection pool) and invoking the same-named method on the returned `RtDbAdminClient`.

### Security

- `SEC-001` — admin token held in JS memory instead of `localStorage` in the dashboard.
- `SEC-004` — WS `is_admin` re-runs per op, closing the admin-revocation lag on open `/sync` connections.
- `SEC-005` — OAuth callback strict origin validator + escaping (self-XSS prevention).
- Admin key compared constant-time (`subtle::ConstantTimeEq`), shared by the header path and the `rtdb-admin.<token>` subprotocol path.
- Per-row `ownerField` pre-checks run inside the serialized transaction with no TOCTOU window; machine tokens and scheduled jobs bypass per-row rules but the db-level gate still runs first.
- `GET /storage/{id}` is the single unauthenticated route — opaque uuid-v7 URLs, revoke by delete, cross-db isolated via `storage_index`.

[Unreleased]: https://github.com/paulrobello/par-rt-db/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/paulrobello/par-rt-db/releases/tag/v0.1.0
