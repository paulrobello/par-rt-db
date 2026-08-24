# par-rt-db-client

Rust client for [par-rt-db](../README.md) — a port of the TypeScript SDK
([`../ts-client/`](../ts-client)) for server-side apps (the par-hack game server
depends on it). Speaks the server's declarative query/transaction DSL over
one-shot HTTP and reactive WebSocket, with an admin control-plane client. No
codegen: you build a `Schema` that serializes to the exact server `SchemaDef`,
and query/mutate results deserialize generically into your own serde structs.

Crate name: `par-rt-db-client` → in Rust, `use par_rt_db_client::...`.

## Table of contents

- [Status / features](#status--features)
- [Install](#install)
- [Quick start (HTTP)](#quick-start-http)
- [Scheduling](#scheduling)
- [Durable workflows](#durable-workflows)
- [Cascade delete + soft delete (FM-33)](#cascade-delete--soft-delete-fm-33)
- [Computed fields](#computed-fields)
- [Schema migration](#schema-migration)
- [File storage](#file-storage)
- [Errors](#errors)
- [Wire contract](#wire-contract)
- [Full API](#full-api)
- [Develop](#develop)

## Status / features

| Feature | Default | Surface |
| --- | --- | --- |
| `http` | yes | `RtDbHttpClient` — typed query / mutate / `auth_me` |
| `ws` | no | `RtDbClient` (`src/ws.rs`) — reactive WebSocket client (live query subscriptions + mutate) |
| `admin` | no | `RtDbAdminClient` (`src/admin/mod.rs`) — `/admin/*` control-plane client: db create/list/push-schema, schema/stats read-back, token mint/revoke/list, db + server-wide admin allowlist CRUD, metrics, hot config GET/PATCH, op-feed `recent`, owner-bypass query/mutate (incl. `include_deleted` for soft-deleted rows), snapshot export/import, schema preview (advisory additive/reject diff), admin schedules CRUD (list/create/cancel/pause/resume), admin storage (list/upload/delete), per-db anonymous-access toggle (SEC-103). Browser-only `login`/`logout`/`/admin/stream` are excluded (the Rust client is a machine client). Construct via `RtDbAdminClient::new(url, admin_key)` or `RtDbHttpClient::admin_client()` (shares the connection pool). The admin methods also remain on `RtDbHttpClient` as `#[deprecated]` re-exports (ARC-121, non-breaking). |
| `in_memory` | no | `InMemoryRtDbClient` (`src/in_memory/`) — in-memory test harness (no network, no Postgres). Ports `ts-client/src/in_memory/`: schema push, mutate (with `mut_id` idempotency), one-shot query DSL (`get`/`first`/`unique`/`count`/`take`/`collect`/`distinct`/`aggregate` + index eq + range + `order` + cursor-keyset `paginate`), `filter()` predicate evaluation (validated against the declared schema — a kind-mismatched value, e.g. a number on a string field, errors with `BAD_REQUEST` instead of silently matching nothing, SEC-126), reactive `subscribe` (re-runs and fires `on_update` on change), `schedule`/`cancel_schedule`/`pause_schedule`/`resume_schedule`/`list_schedules` + a timer-less `tick(now_ms)` (one-shot catches up if past due; cron re-arms by `CRON_STEP_MS = 60_000` and skips missed windows), and the `upload`/`delete_file`/`get_file_metadata`/`get_url` storage stubs. `search` approximates server behavior by mode — websearch operator matching (quoted phrases, `or` unions, `-term` exclusion, FM-31) with optional `_searchSnippet` highlights for `tsquery`, substring + similarity ranking for `trgm` (FM-30); `vector_search` over-approximates (no distance model — every table doc is a candidate, narrowed by the carried `filter`); rejected combinations still throw. |

`core` (wire types, schema/query/mutation builders, error model) compiles with
no features. `[lints.rust] warnings = "deny"` — same zero-warning posture as the
server.

The `http` surface also carries `.filter()` / `.search()` / `.vector_search()` /
`.fields()` query builders (predicate, full-text, vector-similarity terminals, and
field projection — `.fields(&["title"])` keeps the listed user fields per
result doc, system fields always kept, `&[]` an ids-only view, FM-38;
`.search()` takes an optional `mode: "tsquery" | "trgm"` — `trgm` is
substring/autocomplete matching ranked by pg_trgm similarity, FM-30 — and a
`snippet: bool` opt, FM-31: the query text honors web-search operator syntax
server-side (quoted phrases, bare `or`, `-term` exclusion) and `snippet: true`
asks the server to attach a `<mark>`-highlighted `_searchSnippet` fragment to
each hit, tsquery mode only), the
`.hybrid_search()` fused full-text+vector terminal (Reciprocal Rank Fusion),
`.distinct()` (unique values of the index field after the eq prefix, ascending —
NULLs included once and sorted last), and `.aggregate()` (`sum`/`avg`/`min`/
`max`/`count` over the index field after the eq prefix, `null` over an empty
set; `groupBy` shifts to grouped `{key, value}` rows — rows missing the group
field form one `key:null` group sorted last, null agg values are skipped (SQL
semantics), and an all-null group yields `value:null`) terminals, the
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
upsert-insert only; `patch` never re-applies). `updated_at_field(field)`
declares a server-stamped update timestamp (FM-36) — the named `number`/`int64`
field is overwritten with the current epoch-ms on every version-bumping write
(insert, patch, replace, upsert, patchByQuery, cascade setNull), so any
client-supplied value never survives. `auto_increment_field(field)` declares a
server-assigned per-table monotonic counter (FM-37) — the named `int64` field
is stamped with the next per-table sequence value on insert (and upsert's
insert branch), after defaults, overwriting any client-supplied value (and any
`defaults` entry on the field); the decimal-string value is immutable after
insert (a patch / upsert-update patch / patchByQuery supplying a different
value is rejected, and a replace must round-trip the stored value —
omitted/null is filled back in). `.computed(field, expr)` declares a computed
field (ENH-028) — the named field is re-derived from the closed `ValueExpr`
grammar on every write (see below).

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

## Install

`par-rt-db-client` is not published to crates.io — see
[`../docs/RELEASING.md`](../docs/RELEASING.md). Depend on it from a path (inside
this repo) or from git.

Path dependency, for a crate that lives in the `par-rt-db` workspace:

```toml
[dependencies]
par-rt-db-client = { path = "../rust-client", features = ["http"] }
```

Git dependency, for a crate outside this repo. `rust-client` is a member of the
root `[workspace]`, so Cargo resolves the `par-rt-db-client` package name from
the workspace root without a `package` override; pin to a branch or a release
tag (tags are cut per [`../docs/RELEASING.md`](../docs/RELEASING.md)):

```toml
[dependencies]
par-rt-db-client = { git = "https://github.com/paulrobello/par-rt-db", branch = "main", features = ["http"] }
# Or pin to a release tag once one exists:
# par-rt-db-client = { git = "https://github.com/paulrobello/par-rt-db", tag = "v0.x.y", features = ["http"] }
```

## Quick start (HTTP)

```rust
use par_rt_db_client::{Mutation, Order, RtDbHttpClient, TableQuery};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
struct Item { _id: String, name: String, n: i64 }

let token = std::env::var("RTDB_TOKEN").unwrap();
let db = RtDbHttpClient::new("https://rtdb.example.com", "kanban", &token);
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

The by-query steps — `patch_by_query(table, filter, patch, limit)` /
`delete_by_query(table, filter, limit)` — additionally accept the
execution-time-relative `olderThan` filter op:

```rust
use par_rt_db_client::wire::FilterExpr;
// Archive rows whose completedAt is strictly older than now − 7d.
let sweep = FilterExpr::OlderThan { field: "completedAt".into(), ms: 604_800_000 };
let txn = Mutation::new()
    .patch_by_query("workItems", sweep, json!({ "status": "archived" }), None)
    .build();
```

The cutoff is derived from the server clock **at each execution**, so a
scheduled one-shot/cron/interval txn carrying it stays fresh on every fire
with no client re-scheduling (server-side sweeps: archive done rows older
than 7 days, expire claim leases). The op is by-query-only — read/query
filters (`.filter(...)`), `authorize` predicates, partial-index `where`
predicates, and computed `case` whens reject it (`BAD_REQUEST` /
`SCHEMA_VIOLATION` at push) — and requires a declared `number`/`int64`
field (`optional` unwrapped; a null or absent value never matches) with
`ms >= 0`.

`run` deserializes `{result}` into `T` — use the terminal that matches `T`
(`collect`/`take` → `Vec<T>`, `first`/`unique`/`get` → `Option<T>`,
`count` → `i64`, `paginate` → `Paginated<T>`, `distinct` →
`Vec<serde_json::Value>` (or `Vec<String>`/`Vec<f64>` for a homogeneous index
field), `aggregate` → `Option<serde_json::Value>` scalar or
`Vec<AggregateGroup>` when `groupBy`). For many independent queries in
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
        WorkflowStepSpec { txn: Some(txn), await_signal: None, retry: None, sleep_before_ms: None },
        WorkflowStepSpec {
            txn: Some(txn2),
            await_signal: None,
            retry: Some(StepRetry { max_attempts: 5, ..Default::default() }),
            sleep_before_ms: Some(60_000),
        },
    ],
};
let id: String = db.start_workflow(&spec).await?;   // reactive client: WorkflowInfo
db.cancel_workflow(&id).await?;                     // false for a missing/terminal run
let runs: Vec<WorkflowInfo> = db.list_workflows(None).await?;
```

A step is either a txn or an `awaitSignal` wait (exactly one per step).
`WorkflowStepSpec::await_signal(name, timeout_ms)` builds the wait variant: it
parks the run in the non-terminal `waiting` state until a matching signal
arrives — `signal_workflow(id, name, payload)` delivers it on the http, ws,
and admin clients (latest-wins payload, recorded on the step outcome as
`signal`; typed 404/409 errors on unknown id / not waiting / name mismatch).
An optional `timeout_ms` counts as a failed attempt through the step's
`retry` (each re-wait is the full timeout again, no backoff); `None` waits
forever — cancel is the escape. While waiting, `WorkflowInfo` carries
`waiting_for`/`waited_since`.

```rust
let spec = WorkflowSpec {
    name: "gate".into(),
    steps: vec![
        WorkflowStepSpec::await_signal("approve", Some(86_400_000)),
    ],
};
db.signal_workflow(&id, "approve", Some(serde_json::json!({ "approvedBy": "u1" }))).await?;
```

Steps fire as the system principal (a scoped machine token is confined at
submit time); delivery is at-least-once per step, so write idempotent step
txns. A step that exhausts its retries fails the run (terminal). The admin
client adds `list_workflows`/`get_workflow`/`start_workflow`/`cancel_workflow`/
`signal_workflow`/`delete_workflow` over the `/admin/db/{db}/workflows` routes.
Note: the
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

## Computed fields

`.computed(field, expr)` (ENH-028) declares a field the server re-derives on
every write (insert, patch, replace, upsert, patchByQuery, cascade setNull) —
declarative denormalization with no server code, so derived values are
indexable. Any client-supplied value is overwritten (the `ownerField`
authority model); a `null` result removes the key; an evaluation error (e.g.
division by zero, a bad cast) fails the write with `BAD_REQUEST` naming the
field. Build the expression with the `ValueExpr` constructors —
`field`/`literal`/`concat`/`add`/`sub`/`mul`/`div`/`coalesce`/`lower`/`upper`/
`trim`/`cast`/`now`/`case`:

```rust
use par_rt_db_client::schema::{FieldType, Table};
use par_rt_db_client::ValueExpr;

let table = Table::new()
    .field("first", FieldType::String)
    .field("last", FieldType::String)
    .field("fullName", FieldType::String)
    .field("email", FieldType::String)
    .field("handle", FieldType::String)
    .index("by_fullName", &["fullName"])
    // fullName = concat(first, " ", last)
    .computed(
        "fullName",
        ValueExpr::concat([
            ValueExpr::field("first"),
            ValueExpr::literal(" "),
            ValueExpr::field("last"),
        ]),
    )
    // handle = lower(trim(email)) — spaces only, like Postgres btrim
    .computed(
        "handle",
        ValueExpr::lower(ValueExpr::trim(ValueExpr::field("email"))),
    );
```

Push validation rejects (with `BAD_REQUEST`) a computed key that is not a
declared field, targets the table's `ownerField`/`collaboratorsField`/
`autoIncrementField`, references an undeclared or computed field, carries a
principal marker in a `Case.when`, or produces a statically-known kind the
field type rejects (e.g. `concat` on a `number` field — wrap arithmetic in
`Cast::ToString` to store into an `int64` field). `renameField` migrations
rewrite expression references (a keyed entry follows its field);
`dropField` on a referenced field is rejected; `changeType` re-validates. The
`in_memory` harness mirrors the interpreter, stamping, push validation, and
migrate interplay (see `src/in_memory/tests/computed.rs`).

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

let migration = Migration::new()
    .rename_field("items", "title", "summary")
    .change_type("items", "order", FieldType::String, Cast::ToString, Some(json!("0")))
    .set_default("items", "status", json!("backlog"))
    .build();

// dry_run = true previews first — returns the report + derived schema with no writes.
let result = db.admin_client().migrate_schema("kanban", &migration, true).await?;
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

## Full API

The sections above cover the common paths. The rest of the public surface,
by source file:

| Symbol | Feature | Source | Notes |
| --- | --- | --- | --- |
| `RtDbHttpClient` | `http` | `src/http.rs` | One-shot query/mutate/admin-bridge client |
| `RtDbClient` | `ws` | `src/ws.rs` | Reactive WebSocket client — live query subscriptions, presence, mutate |
| `Config` | `ws` | `src/ws.rs` | `RtDbClient` connection/reconnect configuration |
| `ConnectionState` | `ws` | `src/ws.rs` | Reported connection lifecycle state (`connecting`/`open`/`closed`/…) |
| `Subscription` | `ws` | `src/ws.rs` | A live query subscription handle — snapshot + on-change updates |
| `project_optimistic_update` | `ws` | `src/optimistic.rs` | Applies a pending mutation's optimistic projection to a cached query result |
| `InMemoryRtDbClient` | `in_memory` | `src/in_memory/mod.rs` | No-network, no-Postgres test harness mirroring the live clients |
| `RtDbAdminClient::explain_query` | `admin` | `src/admin/mod.rs` | Query-plan explain — mirrors server `admin::observability::explain_query` |
| `RtDbAdminClient::list_webhooks` / `create_webhook` / `delete_webhook` | `admin` | `src/admin/mod.rs` | Webhook subscription CRUD |
| `RtDbAdminClient::get_audit` | `admin` | `src/admin/mod.rs` | Audit log read-back |
| `RtDbAdminClient::list_sessions` / `revoke_session` / `revoke_user_sessions` / `revoke_expired_sessions` | `admin` | `src/admin/mod.rs` | OAuth session administration |
| `RtDbAdminClient::merge_users` | `admin` | `src/admin/mod.rs` | Merges one user's data into another |
| `RtDbAdminClient::backup_now` / `list_backups` / `download_backup` / `delete_backup` / `restore_backup` | `admin` | `src/admin/mod.rs` | Postgres-backed backup/restore — restore always targets a fresh database |
| `RtDbAdminClient::clone_db` | `admin` | `src/admin/mod.rs` | Clones one database's schema and documents into a new database |

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
