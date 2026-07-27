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
| `admin` | no | `/admin/*` control-plane client — db create/list/push-schema, schema/stats read-back, token mint/revoke/list, db + server-wide admin allowlist CRUD, metrics, hot config GET/PATCH, op-feed `recent`, owner-bypass query/mutate, snapshot export/import. Browser-only `login`/`logout`/`/admin/stream` are excluded (the Rust client is a machine client). |

`core` (wire types, schema/query/mutation builders, error model) compiles with
no features. `[lints.rust] warnings = "deny"` — same zero-warning posture as the
server.

The `http` surface also carries `.filter()` / `.search()` / `.vector_search()`
query builders (predicate, full-text, and vector-similarity terminals), the
`mutate_with_retry` precondition-conflict helper, `upsert_by_index` /
`find_one_by_index` shortcuts, and `validate_session_token` for session
validation. `search_index()` declares a full-text index in a `Schema`,
`vector_index()` declares a pgvector-backed vector index (write-maintained
`vector(N)` column + HNSW cosine; embeddings are client-supplied), and
`owner_field()` opts a table into per-row authorization (server-enforced on
read, mutate, and subscription re-run; machine tokens bypass).

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
`count` → `i64`, `paginate` → `Paginated<T>`).

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

## File storage

`RtDbHttpClient` exposes file storage (`upload` / `delete_file` /
`get_file_metadata` / `get_url`):

```rust
use par_rt_db_client::UploadResult;
let up: UploadResult = db.upload(b"file-bytes", Some("image/png")).await?;
let url: String = db.get_url(&up.id);          // public URL — no request made
let meta = db.get_file_metadata(&up.id).await?;
db.delete_file(&up.id).await?;
```

`upload` POSTs raw bytes to `POST /api/storage/{db}` (the client injects its
db); `get_url` returns `{url}/storage/{id}`. Storage is HTTP-only.

## Errors

Every failure is `RtDbError { code, message }` with `ErrorCode` matching the
server's `SCREAMING_SNAKE_CASE` codes (`UNAUTHORIZED`, `PRECONDITION_FAILED`,
…). `retry_on_precondition` is a bounded helper for read-modify-write loops
(`expect_version`/`expect_absent` + retry).

## Wire contract

`src/wire.rs` is the **third** implementation of par-rt-db's protocol contract
(alongside `server/src/protocol.rs` and `ts-client/src/protocol.ts`). They must
stay byte-identical (same serde tags and field names); changing the wire format
on any side is a breaking change unless mirrored on all three. See
[`../CLAUDE.md`](../CLAUDE.md) and the design spec
[`../docs/superpowers/specs/2026-07-22-rust-client-design.md`](../docs/superpowers/specs/2026-07-22-rust-client-design.md).

## Develop

```sh
cargo test --all-features          # full suite (wiremock mocks; no server needed)
cargo build --all-features         # http + ws + admin surfaces compile
cargo build --no-default-features  # core compiles with no features
```

Single test by module/name: `cargo test --lib query`.

The live-server test (`tests/http_integration.rs`) is opt-in — `#[ignore]`, runs
only with `--ignored` when `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY` point
at a running server; it does not need the dev Postgres.
