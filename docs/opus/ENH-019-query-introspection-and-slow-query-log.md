# ENH-019 — Query introspection: compiled SQL, EXPLAIN, and a slow-query log

> **Source**: kanban card `[ENH-019]`, project `par-rt-db`. Derived from the 2026-08-09 Opus audit.
> **Impact**: high (DX) · **Effort**: medium · **Breaking**: no (additive admin surface)

## Goal

Let an operator or app developer answer three questions the product currently makes unanswerable:

1. *What SQL does my DSL query actually become?*
2. *Why is it slow — what does Postgres's planner do with it?*
3. *Which queries were slow last hour, and on which database?*

## Current state

par-rt-db's whole premise is that clients send a declarative JSON DSL and the server compiles it to
SQL (`server/src/query.rs`, 3,334 lines — the largest module in the repo). That compilation is
**completely opaque to the caller**. Verified: grepping `server/src/` for `EXPLAIN`, `slow_query`,
and `slow-query` returns **nothing**.

The consequences show up all over the audit:

- `SEC-104` — a filter over a declared-but-unindexed field compiles to `(doc->>'field')`, which no
  index can serve. **Nothing tells the developer this happened.** They wrote a filter, it worked in
  dev with 50 rows, and it sequential-scans in production.
- `SEC-126` — a type-mismatched comparison compiles to `(doc->>'title')::float8`, raises Postgres
  `22P02` on the first non-numeric row, and the subscription then silently never updates again.
  Seeing the compiled SQL would make that instantly obvious.
- `ARC-102`'s idle-poller finding is the same shape: invisible database work.

The pieces to build on already exist: `metrics.rs` records query latency histograms, `/admin/metrics`
serves JSON, the dashboard has a metrics page, and `admin/docs.rs` already accepts a query DSL body
on `POST /admin/db/{db}/query`.

## Implementation

### Step 1 — Expose the compiled SQL (`POST /admin/db/{db}/explain`)

New admin handler in `server/src/admin/docs.rs` (it already owns admin query/mutate):

- **Request**: the same `Query` DSL body `POST /admin/db/{db}/query` accepts, plus
  `{ "analyze": bool }` (default `false`).
- **Response**:
  ```json
  {
    "sql": "SELECT ... WHERE f_status = $1 ORDER BY f_created_at DESC LIMIT $2",
    "params": ["open", 50],
    "plan": "<EXPLAIN output>",
    "warnings": ["filter on 'title' has no index and compiles to a jsonb extraction"]
  }
  ```
- Reuse the existing compile path so the SQL returned is **the same string the real query runs** —
  refactor `execute_query` so compilation is a separately callable function returning
  `(sql, params)` rather than duplicating the logic. If `QA-002R`'s terminal extraction has landed,
  that seam already exists; build on it rather than adding a parallel compiler.
- `analyze: false` → `EXPLAIN (FORMAT TEXT)`. `analyze: true` → `EXPLAIN (ANALYZE, BUFFERS)` **inside
  a transaction that is rolled back**, so an `ANALYZE` on a query with side effects (there are none
  today, but be defensive) cannot commit.

**Params must still be bound.** Return the parameter list separately; never interpolate values into
the returned SQL string. The audit's strongest positive finding is that no SQL injection exists in
this codebase — do not introduce a formatting path that could become one.

### Step 2 — Compile-time warnings

While compiling, collect advisory warnings into a `Vec<String>`:

- A `filter`/`order` field with **no declared index** → "compiles to a jsonb extraction; no index can
  serve this".
- A comparison whose value type does not match the declared field type → the `SEC-126` case. (If
  `SEC-126`'s fix has landed this is a hard error at subscribe time; keep the warning for one-shot
  queries.)
- A `collect` with no `take`/`paginate` on a table above a row threshold → "unbounded collect".
- `search`/`vectorSearch` with a `filter` over a field outside the index's `filterFields` → post-filter,
  not index-served.

Return them from `/explain`. **Also** attach them to the slow-query log entries in Step 3 — a warning
on a query that is actually slow is the signal; a warning on a fast query is noise.

### Step 3 — Slow-query log

- Boot config `RTDB_SLOW_QUERY_MS` (default `0` = off; suggest `500` in `.env.example`'s commentary).
- In `query.rs`'s execute path, when wall time exceeds the threshold, push a bounded ring-buffer entry
  onto `AppState`: `{ ts_ms, db, table, terminal, duration_ms, sql, params_redacted, warnings }`.
- **In-memory and bounded** (suggest 200 entries, `RTDB_SLOW_QUERY_CAPACITY`). Do **not** write to
  Postgres: the whole point is to observe database pressure, and a write per slow query makes it worse.
  This mirrors how `op_feed` is already an instance-local ring buffer.
- `GET /admin/slow-queries?db=&limit=` returns them, newest-first.
- **Redact parameter values** by default (`$1 = <text>`, `$2 = <int>`), with
  `RTDB_SLOW_QUERY_LOG_PARAMS=true` to opt in to real values. Parameters are user data and this
  endpoint is admin-readable across every tenant database.

### Step 4 — Dashboard page

`dashboard/src/pages/SlowQueriesPage.tsx` — a table (time, db, table, terminal, duration, warnings)
with a row expander showing the SQL and plan, and a "Explain this" action posting to `/explain`.
Register in `App.tsx` and add the row to `dashboard/README.md`'s route table (which `DOC2-025` is
already correcting — coordinate).

**Consume `RtDbAdminClient` from `@par-rt-db/client`, not a hand-rolled fetch.** `ARC-106`/`ARC-107`
are removing the dashboard's parallel admin client and fifth wire-contract copy; do not add a sixth.

### Step 5 — Client mirrors

`/admin/explain` and `/admin/slow-queries` are admin routes, so they belong in the **admin** surface
of all three clients plus the CLI:

- `ts-client/src/admin.ts` — `explainQuery(db, query, opts)`, `getSlowQueries(opts)`
- `rust-client` — `explain_query`, `get_slow_queries` (behind the `admin` feature)
- `python-client/src/par_rt_db/admin.py` — both sync and async classes
- `cli/src/main.rs` — `rtdb explain --db <db> --query <file.json>` is the highest-value half; an
  operator debugging a slow query is at a terminal.

Add wire types to all four protocol files (`server/src/protocol.rs`, `ts-client/src/protocol.ts`,
`rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`) — **byte-identical serde tags and
field names**, per the repo's non-uniform-but-load-bearing casing rule.

## Files to touch

- `server/src/admin/docs.rs` — `/explain` handler
- `server/src/admin/observability.rs` — `/slow-queries` handler (it already owns metrics/ops/subs)
- `server/src/admin/mod.rs` — route registration. **If `SEC-108` (router `route_layer`) has landed,
  add the routes inside the gated sub-router**, not with a per-handler `require_admin`.
- `server/src/query.rs` — extract the compile seam; emit warnings; slow-query hook
- `server/src/lib.rs` — the ring buffer on `AppState`
- `server/src/config.rs` + `.env.example` + `docker-compose.yml` — three keys
- `server/src/protocol.rs` + the three client protocol files
- `ts-client/src/admin.ts`, `rust-client/src/{http.rs,wire/admin.rs}`, `python-client/.../admin.py`, `cli/src/main.rs`
- `dashboard/src/pages/SlowQueriesPage.tsx`, `dashboard/src/App.tsx`
- `README.md`, `server/README.md`, `dashboard/README.md`, `deploy/README.md`, `FEATURE_MATRIX.md`, `CHANGELOG.md`

## Verify

```bash
make -C /Users/probello/Repos/par-rt-db dev-db-up
make -C /Users/probello/Repos/par-rt-db ts-client-build
make -C /Users/probello/Repos/par-rt-db checkall > /tmp/enh019.log 2>&1; echo "EXIT=$?" >> /tmp/enh019.log
grep '^EXIT=' /tmp/enh019.log
make -C /Users/probello/Repos/par-rt-db env-drift-check
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml explain
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml slow_quer
```

**Acceptance criteria** (mirror these onto the card):
1. `make checkall` green.
2. `POST /admin/db/{db}/explain` returns the **same** SQL string the real query path executes —
   proven by a test that compiles a query both ways and asserts equality, not by inspection.
3. The returned `sql` contains `$1`-style placeholders and **no** interpolated literal values; params
   are a separate array.
4. A filter over a declared-but-unindexed field produces a warning naming that field.
5. With `RTDB_SLOW_QUERY_MS=1`, a query appears in `GET /admin/slow-queries`; with the default `0`,
   the list stays empty.
6. Slow-query parameter values are redacted unless `RTDB_SLOW_QUERY_LOG_PARAMS=true`.
7. `explain` and `slow-queries` are mirrored in ts-client, rust-client, and python-client
   (`explain` additionally in the CLI), with matching wire types in all four protocol files.
8. `make env-drift-check` passes.

## Rollback

Additive admin routes plus one in-memory buffer. `RTDB_SLOW_QUERY_MS=0` (the default) disables
collection entirely. No schema change, no wire-breaking change to any existing message. Revert is a
plain `git revert`; clients gain unused methods in the interim, which is harmless.

## Risks

- **The compile seam is the real work.** If `execute_query` cannot cleanly yield `(sql, params)`
  without executing, resist duplicating the compiler — a second compiler that drifts from the first
  makes `/explain` actively misleading, which is worse than not having it. Land `QA-002R`'s extraction
  first if that seam does not already exist.
- **`EXPLAIN ANALYZE` executes the query.** Wrap in a rolled-back transaction and gate it behind the
  explicit `analyze: true` flag.
- **Admin cross-tenant read.** `/slow-queries` exposes query shapes across every database on the
  instance. That matches the existing admin posture (`admin/docs.rs` already reads any db), but the
  parameter redaction default is what keeps it from also exposing user *data*.
