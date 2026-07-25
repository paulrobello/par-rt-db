# par-rt-db Rust Client Crate — Design Spec

**Date:** 2026-07-22
**Status:** Implemented — `par-rt-db-client` v0.1.0 published with full `http` + `ws` + `admin` surfaces, query/mutation DSL, scheduled/cron transactions, file storage, and the `.filter()` / `.search()` / `.vector_search()` builders. Mirrored across FEATURE_MATRIX rows #1–#21 wherever "Mirrored end-to-end" calls out `rust-client` (notably the "Admin control plane" row in §1 and rows #11 / #15 / #17 / #16 / #9 in §2). Current source of truth: `rust-client/src/` and `FEATURE_MATRIX.md`.
**Repo:** `~/Repos/par-rt-db` (crate lives in `rust-client/`)
**Kanban:** par-rt-db → "Build Rust client crate (for par-hack game server)" (high)
**End goal:** functional parity with the TypeScript client (`client/`, `@par-rt-db/client`) **minus the React bindings**.

## Purpose

A Rust client crate that speaks par-rt-db's JSON wire protocol over both one-shot
HTTP and the reactive WebSocket `/sync`, with a schema/query/mutation builder DSL.
It is the third implementation of the wire contract (server `protocol.rs` is the
first, TS `protocol.ts` the second); all three must stay byte-identical.

The motivating consumer is **par-hack** (`~/Repos/par-hack`), an invite-only
multiplayer "world of simulated Linux boxes." Per its PRD (§6, Phase M2), the Rust
game server (axum/tokio) is the **sole par-rt-db client via machine token** and
needs **query/mutate/auth-me over the HTTP API** plus a hydrate/write-through
mapping to its `fsnodes`/`boxes`/`accounts`/`networks`/`players`/`invites` tables.
That HTTP path is the critical path that unblocks par-hack M2; the WS reactive
client and admin control plane complete the full surface for other Rust apps.

This spec covers the crate only. Server/protocol changes are out of scope — the
client speaks the protocol as it exists today.

## Decisions (settled during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Scope (v1) | Full parity minus React | User chose to build the whole surface up front rather than an HTTP-first slice. |
| Typing strategy | **C — Hybrid**: runtime schema/query/mutation builders + serde-generic result deserialization | Maps straight onto par-hack's existing serde structs; no proc-macro risk in an already-large build; wire-identical and purely additive so a future derive macro can layer on. |
| Result typing | `query::<T: DeserializeOwned>(...)` deserializes the `QueryResult` payload into user structs; table/index names are runtime strings (validated server-side) | Server treats schema as runtime data; typos surface immediately as `SCHEMA_VIOLATION`/`BAD_REQUEST`. |
| Reactive API | `subscribe(q) -> tokio::sync::watch::Receiver<T>` (latest-value + updates, multi-consumer) + optional `subscribe_stream` adapter | A live query *is* a current-value-that-updates; `watch` fits natively. |
| Location | New `rust-client/` dir in this repo | The wire protocol it mirrors lives here. |
| Packaging | Standalone Cargo package (not a workspace member) | Mirrors how `client/` (TS) is its own package; avoids risky workspace conversion of `server/`. |
| Features | core (wire + DSL) always on; `http`, `ws`, `admin` opt-in; default `["http"]` | par-hack gets a lean dep tree; full parity = all features. |
| Auth | Machine bearer tokens for the data/WS API; `auth_me(session_token)` helper for par-hack's player-session validation | Game server = trusted machine; players authenticate via GitHub OAuth validated server-side. |
| Async stack | tokio 1, reqwest 0.12 (rustls-tls), tokio-tungstenite 0.26, serde 1, thiserror 2, base64 0.22 | Matches server + par-hack (`persistence` already uses serde/async-trait/thiserror/tokio). |
| Wire types | Re-declared byte-identical (third contract copy) with round-trip parity tests | Same model as `protocol.ts`; keeps the three-way contract honest. |

## Wire contract (third implementation)

The crate re-declares these types with serde attributes that reproduce the server's
wire bytes exactly. Discriminator-key and casing rules differ per type and are
load-bearing.

**Messages** — `#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]`.
`ClientMessage` is `deny_unknown_fields` (extra keys close the WS):

```
ClientMessage: auth{token, db} | subscribe{queryId, query} | unsubscribe{queryId}
             | mutate{mutId, idempotencyKey?, txn} | ping
ServerMessage: authOk{user} | authErr{error} | queryUpdate{queryId, result}
             | mutateOk{mutId, results[]} | mutateErr{mutId, error}
             | subscribeErr{queryId, error} | pong
AuthedUser:    { kind: "user"|"machine", email?, name? }
```

**Query** — plain struct, field names **snake_case** on the wire, all fields
`#[serde(default)]`:

```
Query: { table, get?, index?, eq[]?, gt?|gte?, lt?|lte?, order?("asc"|"desc"),
         take?(u32, max 4096), unique?(bool), first?(bool), count?(bool),
         paginate?: { cursor?, numItems } }
```

`QueryResult` is **untagged** (serialize-only on the server; the client deserializes
by shape): an object or `null` (`Doc`), an array (`Docs`), a bare number (`Count`),
or `{ docs[], nextCursor? }` (`Paginated`). The client maps these onto `T` /
`Vec<T>` / `i64` / a paginated struct respectively via the generic result typing.

**Transaction** — `{ steps: Step[] }`, max 256 steps. `Step` is
`#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]`:

```
insert{table, doc} | patch{table, id, fields} | replace{table, id, doc}
| delete{table, id} | expectVersion{table, id, version:i64}
| expectAbsent{table, index, eq[]} | upsert{table, index, eq[], insert, patch}
```

`doc`/`fields`/`insert`/`patch` are JSON objects (`serde_json::Map`). Per-step
results (positional, aligned with `steps`): `insert → {id}`, `upsert → {id, inserted}`,
`patch/replace/delete/expectVersion/expectAbsent → null`.

**Schema** — `SchemaDef = { tables: { name: { fields: { name: FieldType }, indexes?: [{ name, fields[] }] } } }`.
`FieldType` is `#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]`:
`string | number | boolean | null | id{table} | literal{value} | optional{inner} |
union{variants[]} | array{element} | object{fields} | int64 | bytes | any |
record{value}`. (`int64` and `Id` are JSON **strings** on the wire — never numbers —
to stay exact across the full `i64`/id range.)

**Errors** — `{ code, message }`. `code` is `SCREAMING_SNAKE_CASE`:
`UNAUTHORIZED(401) | FORBIDDEN(403) | NOT_FOUND(404) | SCHEMA_VIOLATION(422) |
PRECONDITION_FAILED(409) | BAD_REQUEST(400) | INTERNAL(500)`. Same envelope on HTTP
bodies and inside WS error frames.

**Round-trip parity tests** assert the crate's serialized JSON equals the server/TS
fixture JSON for a battery of messages, queries, transactions, and schemas. This is
the safety net for the three-way contract.

## Crate structure

`rust-client/` — standalone Cargo package `par-rt-db-client`, edition 2024, stable
toolchain, `clippy` `-D warnings` + `#![deny(warnings)]` (matches par-hack's lint
posture). Module layout:

```
rust-client/
  Cargo.toml
  Makefile                 # build/test/lint/fmt/typecheck/checkall/pre-commit
  src/
    lib.rs                 # re-exports, feature gating
    error.rs               # RtDbError + ErrorCode + retry_on_precondition
    wire.rs                # ClientMessage/ServerMessage/AuthedUser (the "type"-tagged half)
    query.rs               # Query, Order, Paginate, QueryResult untagged mapping, TableQuery builder
    mutation.rs            # Transaction, Step, StepResult, Mutation builder
    schema.rs              # SchemaDef, FieldType, Schema builder
    cursor.rs              # opaque cursor encode/decode (base64 of JSON array) — parity helpers
    http.rs                # RtDbHttpClient          [feature = http]
    client.rs              # RtDbClient (reactive WS) [feature = ws]
    admin.rs               # RtDbAdminClient         [feature = admin]
  tests/
    wire_parity.rs         # round-trip JSON == fixtures
    dsl.rs                 # schema/query/mutation builder shapes
    http_integration.rs    # opt-in: live server (RTDB_TEST_SERVER_URL + admin key)
    ws_integration.rs      # opt-in: live server
    admin_integration.rs   # opt-in: live server
    common/mod.rs          # shared test harness (provision db, mint token, push schema)
```

The root `Makefile` gains a `rust-client` target so `make checkall` runs
`server/` + `client/` + `rust-client/`. Feature flags: `default = ["http"]`;
`http` enables `reqwest`; `ws` enables `tokio-tungstenite` + `futures-util`;
`admin` enables `reqwest`. Core (wire + DSL + schema) compiles with no features.

## DSL — schema / query / mutation (Approach C)

```rust
// Schema → exact SchemaDef for push_schema
let schema = Schema::builder()
    .table("boxes", |t| t
        .field("owner_id", FieldType::id("players"))
        .field("status", FieldType::string())
        .field("fsroot", FieldType::id("fsnodes"))
        .index("by_status", ["status"])
        .index("by_owner", ["owner_id"]))
    .table("players", |t| t.field("email", FieldType::string()))
    .build();

// Query → typed result
let active: Vec<Box> = http.query::<Box>("by_status").eq("active").take(50).collect().await?;
let one: Option<Box> = http.query::<Box>("by_status").eq("active").first().await?;
let n: i64           = http.query::<Box>("by_status").eq("active").count().await?;
let by_id: Option<Box> = http.get::<Box>("0123...").await?;            // point read
let page = http.query::<Box>("by_status").eq("active").order(Asc).paginate(None, 100).await?;

// Mutation → step results
let res: Vec<StepResult> = http.mutate(
    Mutation::new()
        .insert("boxes", json!({ "owner_id": pid, "status": "active" }))
        .patch("boxes", box_id, json!({ "status": "idle" }))
        .expect_version("boxes", box_id, 7)
        .upsert("boxes", ("by_owner", [pid]), insert_doc, patch_doc)
        .build(),
    Some(idempotency_key), // optional safe-retry key
).await?;
```

`query::<T>()` carries `T: DeserializeOwned`; terminals pick the deserialization:
`.collect() → Vec<T>`, `.first()/.unique() → Option<T>`, `.count() → i64`,
`.paginate() → Paginated<T>`. Table and index names are `&str` (runtime). The
builder enforces client-side only what is locally knowable (e.g. range/terminal
mutual exclusion mirrors the server's rules to fail fast); everything schema-shaped
is validated by the server.

## HTTP client — `http` feature (par-hack critical path)

`RtDbHttpClient::new(url, db, token)` — `token` is a machine token (or a user
session token); sent as `Authorization: Bearer <token>` on every call.

| Method | Endpoint | Body | Returns |
|---|---|---|---|
| `query::<T>(q)` | `POST /api/query` | `{ db, query }` | `T` (from `{ result }`) |
| `mutate(txn, idempotency_key?)` | `POST /api/mutate` | `{ db, txn, idempotencyKey? }` | `Vec<StepResult>` (from `{ results }`) |
| `auth_me(session_token)` | `GET /auth/me` | — (bearer = session) | `AuthedUser` (from `{ user }`); machine tokens rejected 401 |

reqwest with `default-features = false`, features `["json", "rustls-tls"]`. Non-2xx
responses whose body parses as `{code, message}` become `RtDbError`; otherwise
`INTERNAL "request failed with status N"`.

## Reactive WS client — `ws` feature

`RtDbClient::new(url, db, get_token)` where `get_token` is an **async** token
provider `Fn() -> impl Future<Output = Option<String>>` (async so a refreshed token
can be fetched on reconnect), called on every (re)open — mirroring the TS `getToken`.
The `url` origin's scheme is flipped `http→ws`/`https→wss` and `/sync` appended
(plain WebSocket; there is no subprotocol and no query-string auth).

Lifecycle mirrors the TS client exactly:
- **Handshake**: on open, send `{type:"auth", token, db}` as the first frame; await
  `authOk{user}` / `authErr`. WS close code `4401` (auth failed) is **terminal** — no
  reconnect; `4400` (protocol violation / oversized / rate limit) reconnects.
- **Reconnect**: jittered exponential backoff (`min(max_ms, base_ms * 2^attempt) *
  (0.5 + rand*0.5)`), attempt reset on successful `authOk`; resubscribe every active
  query.
- **Keepalive**: send `{type:"ping"}` on an interval; if no `pong` within 2× the
  interval, close with `4000` and reconnect.
- **Generation guard**: a single `AtomicU64` bumped on every (re)open and on
  `close()`. Every async wakeup (token future, reconnect timer, keepalive timer)
  captures its generation and aborts if it has advanced — preventing stale callbacks
  from opening a duplicate socket (the mypi/par-mmo pattern from the vault).
- **Subscriptions**: `subscribe(query) -> watch::Receiver<T>`; dedup by the
  canonical serialized query (one wire `subscribe` per unique shape, many receivers);
  `queryId` generated from a per-client `AtomicU64` counter (`sub-{n}`). The initial
  `queryUpdate` delivers the first value (receiver starts `None` until then).
  `unsubscribe` on the last receiver for a shape sends `{type:"unsubscribe"}`.
- **Mutations — at-most-once**: `mutate(txn, idempotency_key?) -> Future<Result<Vec<StepResult>>>`.
  `mutId` is a per-client counter (`mut-{n}`), pure reply correlation (never
  persisted); the optional `idempotency_key` is the wire `idempotencyKey` for safe
  retry. In-flight (sent, unacked) mutations are **rejected** on close ("connection
  closed before acknowledgment"); never-sent mutations are re-queued for the next
  connection. Never auto-resent.

`subscribe_stream(query) -> impl Stream<Item = T>` is a thin adapter over the watch
receiver for pipeline-style consumption.

## Admin control plane — `admin` feature

`RtDbAdminClient::new(url, admin_key)` — `Authorization: Bearer <admin_key>` on
every call (the configured `RTDB_ADMIN_KEY`, constant-time compared server-side).

| Method | Endpoint | Body | Returns |
|---|---|---|---|
| `create_db(name)` | `POST /admin/create-db` | `{ name }` | `()` (`{ok}`) |
| `push_schema(db, schema)` | `POST /admin/push-schema` | `{ db, schema }` | `()` |
| `list_dbs()` | `GET /admin/dbs` | — | `Vec<String>` (`{databases}`) |
| `mint_token(db, name)` | `POST /admin/mint-token` | `{ db, name }` | `{ token_id, token }` |
| `revoke_token(token_id)` | `POST /admin/revoke-token` | `{ tokenId }` | `()` |
| `allowlist_add/remove(db, email)` | `POST /admin/allowlist` | `{ db, action, email }` | `()` |
| `allowlist_list(db)` | `GET /admin/allowlist?db=` | — | `Vec<String>` (`{emails}`) |
| `export_db(db)` | `GET /admin/export-db?db=` | — | JSONL `String` |
| `import_db(db, jsonl)` | `POST /admin/import-db?db=` | JSONL body | `()` |

## Error handling

`RtDbError` (`thiserror::Error`, `code: ErrorCode`, `message: String`) is the single
error type across all features. It is constructed from HTTP non-2xx bodies and from
WS error frames (`authErr`/`mutateErr`/`subscribeErr`). `ErrorCode` serde-serializes
`SCREAMING_SNAKE_CASE`. `retry_on_precondition<F>(f, retries=4)` retries a
read-modify-write closure only on `PRECONDITION_FAILED` — the SDK's sole automatic
retry, mirroring TS `retry.ts`.

## Testing

- **Unit (always run)**: wire round-trip parity vs fixtures; DSL builder shapes
  (schema/query/mutation serialize to the exact expected JSON); cursor codec
  round-trip; `QueryResult` untagged deserialization for each shape; error mapping.
- **Integration (opt-in)** — gated by `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`
  (mirrors the TS `tests/integration/**` opt-in model). Harness: spin up the dev
  server + `make dev-db-up`, `create_db` a uniquely-named `t<uuid>` db, `push_schema`,
  `mint_token`, then exercise: HTTP query/mutate/auth-me end-to-end; WS
  subscribe→`queryUpdate`, reconnect-resubscribe, at-most-once mutate across a
  dropped connection, generation guard; admin round-trips incl. export/import.
  Tests isolate by unique db name and never touch a db they didn't create.
- `make checkall` (in `rust-client/`) = fmt-check + clippy `-D warnings` + tests.

## Phasing

The build lands the par-hack critical path first, then completes the surface:

1. **Wire + DSL core** (no features): `error`, `wire`, `query`, `mutation`, `schema`,
   `cursor` + parity/builder unit tests.
2. **HTTP** (`http`): `RtDbHttpClient` query/mutate/auth-me + integration tests.
3. **Admin** (`admin`): `RtDbAdminClient` + integration tests.
4. **WS reactive** (`ws`): `RtDbClient` with generation guard, at-most-once, dedup,
   watch API + integration tests.

par-hack can begin M2 integration after phase 2.

## Out of scope (v1)

- React bindings (N/A in Rust).
- A derive-macro typing layer (Approach B) — purely additive; a later follow-on that
  layers over this crate without any wire change.
- New server endpoints or protocol changes — the client speaks the protocol as-is.
- An in-memory test harness / fake client (separate backlog item #19).

## Success criteria

1. `make checkall` green for `rust-client/` (fmt-check + clippy `-D warnings` + tests).
2. Wire round-trip parity tests pass against the server/TS fixtures (three-way
   contract holds).
3. Opt-in integration suite passes against a live dev server + dev-db for HTTP,
   WS, and admin.
4. par-hack's `persistence` crate can depend on `par-rt-db-client` (default
   `["http"]`), push a schema, and run typed query/mutate/auth-me round-trips
   (unblocks par-hack M2).

## Future (explicitly deferred)

Derive-macro typed schema (Approach B); client-side optimistic updates (#12); an
in-memory fake client for app unit tests (#19); anything the server doesn't yet
expose (full-text search, vector search, scheduling) — the client grows those
terminals only after the server ships them.
