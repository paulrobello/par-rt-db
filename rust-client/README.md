# par-rt-db-client

Rust client for [par-rt-db](../README.md) — a port of the TypeScript SDK
([`../ts-client/`](../ts-client)) for server-side apps (the par-hack game server
depends on it). Speaks the server's declarative query/transaction DSL over
one-shot HTTP and reactive WebSocket, with an admin control-plane client. No
codegen: you build a `Schema` that serializes to the exact server `SchemaDef`,
and query/mutate results deserialize generically into your own serde structs.

Crate name: `par-rt-db-client` → in Rust, `use par_rt_db_client::...`.

## Status / features

| Feature | Default | Surface |
| --- | --- | --- |
| `http` | yes | `RtDbHttpClient` — typed query / mutate / `auth_me` |
| `ws` | no | `RtDbClient` (`src/ws.rs`) — reactive WebSocket client (live query subscriptions + mutate) |
| `admin` | no | `RtDbAdminClient` (`src/admin.rs`) — `/admin/*` control-plane client: db create/list/push-schema, schema/stats read-back, token mint/revoke/list, db + server-wide admin allowlist CRUD, metrics, hot config GET/PATCH, op-feed `recent`, owner-bypass query/mutate (incl. `include_deleted` for soft-deleted rows), snapshot export/import, schema preview (advisory additive/reject diff), admin schedules CRUD (list/create/cancel/pause/resume), admin storage (list/upload/delete), per-db anonymous-access toggle (SEC-103). Browser-only `login`/`logout`/`/admin/stream` are excluded (the Rust client is a machine client). Construct via `RtDbAdminClient::new(url, admin_key)` or `RtDbHttpClient::admin_client()` (shares the connection pool). The admin methods also remain on `RtDbHttpClient` as `#[deprecated]` re-exports (ARC-121, non-breaking). |
| `in_memory` | no | `InMemoryRtDbClient` (`src/in_memory/`) — in-memory test harness (no network, no Postgres). Ports `ts-client/src/in_memory/`: schema push, mutate (with `mut_id` idempotency), one-shot query DSL (`get`/`first`/`unique`/`count`/`take`/`collect` + index eq + range + `order` + cursor-keyset `paginate`), `filter()` predicate evaluation, reactive `subscribe` (re-runs and fires `on_update` on change), `schedule`/`cancel_schedule`/`pause_schedule`/`resume_schedule`/`list_schedules` + a timer-less `tick(now_ms)` (one-shot catches up if past due; cron re-arms by `CRON_STEP_MS = 60_000` and skips missed windows), and the `upload`/`delete_file`/`get_file_metadata`/`get_url` storage stubs. `search` approximates server behavior by mode — websearch operator matching (quoted phrases, `or` unions, `-term` exclusion, FM-31) with optional `_searchSnippet` highlights for `tsquery`, substring + similarity ranking for `trgm` (FM-30); `vector_search` over-approximates (no distance model — every table doc is a candidate, narrowed by the carried `filter`); rejected combinations still throw. |

`core` (wire types, schema/query/mutation builders, error model) compiles with
no features. `[lints.rust] warnings = "deny"` — same zero-warning posture as the
server.

The `http` surface also carries `.filter()` / `.search()` / `.vector_search()`
query builders (predicate, full-text, and vector-similarity terminals;
`.search()` takes an optional `mode: "tsquery" | "trgm"` — `trgm` is
substring/autocomplete matching ranked by pg_trgm similarity, FM-30 — and a
`snippet: bool` opt, FM-31: the query text honors web-search operator syntax
server-side (quoted phrases, bare `or`, `-term` exclusion) and `snippet: true`
asks the server to attach a `<mark>`-highlighted `_searchSnippet` fragment to
each hit, tsquery mode only), the
`.hybrid_search()` fused full-text+vector terminal (Reciprocal Rank Fusion),
`.distinct()` (collapse duplicates on a field set), and `.aggregate()` (grouped
`sum`/`avg`/`min`/`max`/`count`) terminals, the
`mutate_with_retry` precondition-conflict helper, `upsert_by_index` /
`find_one_by_index` shortcuts, and `validate_session_token` for session
validation. `search_index()` declares a full-text index in a `Schema`,
`vector_index()` declares a pgvector-backed vector index (write-maintained
`vector(N)` column + HNSW over a configurable distance metric — cosine by
default, also L2 / inner-product; embeddings are client-supplied), and
`owner_field()` opts a table into per-row authorization (server-enforced on
read, mutate, and subscription re-run; machine tokens bypass), and
`defaults(&[(field, value), ...])` declares field-level default values (FM-32) —
stamped onto a **new** document that omits the key (insert / replace /
upsert-insert only; `patch` never re-applies).

### In-memory test harness (feature `in_memory`)

`InMemoryRtDbClient` mirrors the server's schema/query/txn/step-result semantics
with no network and no Postgres — a direct port of `ts-client/src/in_memory/`.
It exposes the same data surface as the live clients (`push_schema`,
`run`/`run_query`, `mutate` with `mut_id` idempotency, and reactive `subscribe`)
so a unit test can swap it in behind a shared interface. Atomic rollback on step
failure, system-field merging at read time, and cursor-keyset pagination all
behave like the server. The harness is opt-in (gates a `sha2` dependency for
SHA-256 file digests); `search` approximates ranking client-side
(websearch-operator matching — quoted phrases require adjacency, `or` unions,
`-term` excludes, plain terms ANDed — with `<mark>` snippet excerpts when
`snippet: true` for `tsquery`, FM-31, plus substring +
`query.len()/field.len()` similarity ranking for `trgm`, FM-30) while
`vector_search` has no distance model and
over-approximates (every table doc is a candidate, narrowed by the carried
`filter`); storage (`upload`/`delete_file`/
`get_file_metadata`/`get_url`) is a real in-memory `HashMap` stub — both match
the TS harness's surface so app-level storage flows can be exercised with no
network.

## Quick start (HTTP)

```toml
[dependencies]
par-rt-db-client = { version = "0.1", features = ["http"] }
```

```rust
use par_rt_db_client::{Mutation, Order, RtDbHttpClient, TableQuery};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct Item { _id: String, name: String, n: i64 }

# let token = std::env::var("RTDB_TOKEN").unwrap();
# let db = RtDbHttpClient::new("https://rtdb.pardev.net", "kanban", &token);
// Ordered scan into Vec<Item>.
let rows: Vec<Item> = db.run(
    TableQuery::new("items").with_index("by_n", &[]).order(Order::Asc).take(10),
).await?;

// Atomic multi-step transaction; one StepResult per step.
let txn = Mutation::new()
    .insert("items", json!({ "name": "x", "n": 1 }))
    .patch("items", "i1", json!({ "n": 2 }))
    .delete("items", "i2")
    .build();
let _results = db.mutate(&txn, None).await?; // idempotency key is optional

// Point read -> Option<Item>.
let _one: Option<Item> = db.get("items", "i1").await?;
```

`run` deserializes `{result}` into `T` — use the terminal that matches `T`
(`collect`/`take` → `Vec<T>`, `first`/`unique`/`get` → `Option<T>`,
`count` → `i64`, `paginate` → `Paginated<T>`). For many independent queries in
one round trip, `batch_query(&[Query])` fans out via `POST /api/query-batch` and
returns a length-aligned `Vec<BatchQueryOutcome>` (each slot's `result` is a raw
`serde_json::Value` because a batch spans terminals; a per-query error is that
slot's `{ok:false,error}` and never fails the call).

## Scheduling

`RtDbHttpClient` and the reactive `RtDbClient` (`ws` feature) both expose
scheduled/cron transactions. `when` is `ScheduleWhen::AfterMs { ms }`,
`RunAt { ms }`, or `Cron { expr }` (5-field, min-first, UTC; the server
validates); wire shapes mirror the server byte-for-byte (see `src/wire.rs`).

```rust
use par_rt_db_client::{ScheduleWhen, ScheduleInfo};
let id: String = db.schedule(&txn, ScheduleWhen::AfterMs { ms: 60_000 }).await?;
db.cancel_schedule(&id).await?;      // …or pause_schedule / resume_schedule
let jobs: Vec<ScheduleInfo> = db.list_schedules().await?;
```

## Durable workflows

Durable declarative workflows (FM-29): a named spec of steps — each an
ordinary `Transaction` plus optional `StepRetry` and `sleepBeforeMs` — that
the server advances durably. `RtDbHttpClient::start_workflow` returns the new
run id; the reactive `RtDbClient` (`ws` feature) trio returns `WorkflowInfo`
directly; `Mutation::start_workflow(spec)` starts a run as a txn step,
atomic with the txn's writes.

```rust
use par_rt_db_client::{StepRetry, WorkflowInfo, WorkflowSpec, WorkflowStepSpec};

let spec = WorkflowSpec {
    name: "onboard".into(),
    steps: vec![
        WorkflowStepSpec { txn, retry: None, sleep_before_ms: None },
        WorkflowStepSpec {
            txn: txn2,
            retry: Some(StepRetry { max_attempts: 5, ..Default::default() }),
            sleep_before_ms: Some(60_000),
        },
    ],
};
let id: String = db.start_workflow(&spec).await?;   // reactive client: WorkflowInfo
db.cancel_workflow(&id).await?;                     // false for a missing/terminal run
let runs: Vec<WorkflowInfo> = db.list_workflows(None).await?;
```

Steps fire as the system principal (a scoped machine token is confined at
submit time); delivery is at-least-once per step, so write idempotent step
txns. A step that exhausts its retries fails the run (terminal). The admin
client adds `list_workflows`/`get_workflow`/`start_workflow`/`cancel_workflow`/
`delete_workflow` over the `/admin/db/{db}/workflows` routes. Note: the
in-memory test harness does NOT model the workflow engine (its workflow arms
return `Internal` errors) — test workflow flows against a live server.

## Cascade delete + soft delete (FM-33)

Declare app-level foreign keys with `.on_delete()` on an id field — the
server executes the action inside the deleting transaction (`cascade`
recurses, `restrict` conflicts with a `table.field`-naming message, `setNull`
clears the reference — legal only on an `optional` id). One delete may write
several tables; cycles terminate and a 10,000-row cascade budget aborts
atomically. `.soft_delete()` on a table swaps removal for a `deleted_at`
stamp: the row disappears from every read, write lookup, and unique index
(same key re-insertable) and comes back via `.undelete()` — idempotent,
`NotFound` when absent, `BadRequest` on a table that doesn't declare
`softDelete`.

```rust
use par_rt_db_client::schema::{FieldType, OnDeleteAction, Table};

let table = Table::new()
    .field("note", FieldType::String)
    .field("parentId", FieldType::id("parents").on_delete(OnDeleteAction::Cascade))
    .soft_delete();
let txn = Mutation::new().undelete("children", &id).build();
```

Adding or changing an `onDelete` action (and adding the `softDelete` flag)
is an additive schema push — runtime delete behavior only, no stored-row
change. The TTL reaper hard-deletes expired rows even on a `softDelete`
table and honors `onDelete` children. The `in_memory` harness mirrors all
of this (see `src/in_memory/tests/`).

## Schema migration

Destructive/type-changing schema transformations are a deliberate admin operation,
separate from the additive `push_schema`. Build a `Migration` (feature `admin`)
and apply it via `db.admin_client().migrate_schema(...)` — `POST /admin/db/{db}/migrate`
runs the directives transactionally inside the committer, so live queries, the op
feed, audit, and webhooks all fire. (`RtDbHttpClient::migrate_schema` still exists
but is `#[deprecated]` — ARC-121 — prefer the admin client.)

```rust
use par_rt_db_client::{Cast, FieldType, Migration};
use serde_json::json;

let result = db.admin_client().migrate_schema("kanban", &Migration::new()
    .rename_field("items", "title", "summary")
    .change_type("items", "order", FieldType::String, Cast::ToString, Some(json!("0")))
    .set_default("items", "status", json!("backlog"))
    .dry_run(true)   // preview first — returns the report + derived schema
    .build_request()
    .directives, true).await?;
// re-run with dry_run = false to apply
```

`change_type` takes a closed `Cast` (`ToString`/`ToNumber`/`ToInt64`/`ToBoolean`);
the optional `default` substitutes for un-coercible rows (without it a single bad
value rolls the whole migrate back atomically). `eval_expr` is the scoped raw-SQL
escape hatch (one table's `doc` jsonb, no joins/DDL). See
[`docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md`](../docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md).

## File storage

`RtDbHttpClient` exposes file storage (`upload` / `upload_stream` /
`delete_file` / `get_file_metadata` / `get_url` / `get_signed_url` /
`transform_url`):

```rust
use par_rt_db_client::UploadResult;
let up: UploadResult = db.upload(b"file-bytes", Some("image/png")).await?;
let url: String = db.get_url(&up.id);          // public URL — no request made
let meta = db.get_file_metadata(&up.id).await?;
db.delete_file(&up.id).await?;
```

`upload` POSTs raw bytes to `POST /api/storage/{db}` (the client injects its
db); `upload_stream` forwards a `TryStream` chunk-by-chunk instead of buffering
the whole file (ENH-021 — one chunk in flight at a time); `get_url` returns
`{url}/storage/{id}`; `transform_url(id, &TransformOpts)` appends image-transform
params to that URL (no request made); `get_signed_url` calls
`GET /api/storage/{db}/{id}/signed-url?ttlSeconds=` to mint a signed,
time-limited URL (returns `{url, expiresAt}`). Storage is HTTP-only.

## Errors

Every failure is `RtDbError { code, message }` with `ErrorCode` matching the
server's `SCREAMING_SNAKE_CASE` codes (`UNAUTHORIZED`, `PRECONDITION_FAILED`,
…). `retry_on_precondition` is a bounded helper for read-modify-write loops
(`expect_version`/`expect_absent` + retry).

## Wire contract

`src/wire.rs` is one of **four** implementations of par-rt-db's protocol
contract (alongside `server/src/protocol.rs`, `ts-client/src/protocol.ts`, and
`python-client/src/par_rt_db/wire.py`). They must stay byte-identical (same
serde tags and field names); changing the wire format on any side is a
breaking change unless mirrored on all four. See
[`../CLAUDE.md`](../CLAUDE.md) and the design spec
[`../docs/superpowers/specs/2026-07-22-rust-client-design.md`](../docs/superpowers/specs/2026-07-22-rust-client-design.md).

## Develop

```sh
cargo test --all-features          # full suite (wiremock mocks; in-memory harness; no server needed)
cargo build --all-features         # http + ws + admin + in_memory surfaces compile
cargo build --no-default-features  # core compiles with no features
```

Single test by module/name: `cargo test --lib query`.

The live-server test (`tests/http_integration.rs`) is opt-in — `#[ignore]`, runs
only with `--ignored` when `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY` point
at a running server; it does not need the dev Postgres.
