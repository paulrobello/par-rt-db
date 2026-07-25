# par-rt-db Python Client — Design Spec

**Date:** 2026-07-25
**Status:** Approved approach (A), pre-implementation
**Repo:** `~/Repos/par-rt-db` (package lives in new `python-client/`)
**End goal:** a general-purpose Python SDK at functional parity with the TypeScript and
Rust clients (**minus React bindings and browser OAuth helpers**) — a fourth
implementation of par-rt-db's JSON wire contract.

## Purpose

A Python client that speaks par-rt-db's wire protocol over both one-shot HTTP and the
reactive WebSocket `/sync`, with a Pydantic-v2 schema/query/mutation builder DSL. It is
the **fourth implementation** of the wire contract (server `protocol.rs` is first, TS
`protocol.ts` second, Rust `wire.rs` third); all four must stay byte-identical. The
client mirrors the **full server surface** (so it is complete from day one, including
admin endpoints the TS client currently lacks).

It targets CPython **3.12+**, is async-first (`asyncio`, `httpx`, `websockets`), and
uses **Pydantic v2** for wire types, schema, and result models. Machine bearer tokens
are the primary auth story for Python (a server-side language); OAuth popup flows are
browser-only and out of scope (matching the Rust client).

This spec covers the package only. Server/protocol changes are out of scope — the
client speaks the protocol as it exists today.

## Decisions (settled during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Scope (v1) | Full parity minus React/browser-OAuth | User chose "general-purpose SDK" + "feature complete"; build the whole surface up front (same call the Rust client made). |
| Use case | General-purpose SDK for any Python app | Drives async + full reactive parity + admin + in-memory harness. |
| Type model | **Pydantic v2** | User choice. Wire types + schema + results as `BaseModel`; validation, ergonomics, JSON schema, the Python SDK standard. Results deserialized into a user `BaseModel` via a `TypeVar` bound to a small protocol (dataclasses/`dict` also usable). |
| Concurrency | **asyncio-first, async-only v1** | Reactive subscriptions are inherently async. Sync wrappers are purely additive and deferred (Approach B follow-on). |
| Result typing | Terminals take `model: type[T]` where `T` is a Pydantic `BaseModel` (parsed via `model_validate`); passing `dict` returns raw `dict[str, Any]` untyped. Shape picked by the terminal used | Mirrors Rust's generic `query::<T>()`; schema is runtime data, table/index names are runtime strings validated server-side. |
| Reactive API | `async for value in client.subscribe(query)` + `.current()` latest-value | A live query is a current-value-that-updates; an async iterator is the Pythonic analog of Rust's `watch::Receiver`. |
| Location | New `python-client/` dir in this repo | The wire protocol it mirrors lives here. |
| Packaging | `uv`-managed, PEP 621 `pyproject.toml`, dist `par-rt-db`, `requires-python = ">=3.12"`, MIT | Matches the guide (`uv`, 3.12 target as floor). |
| Extras | core (wire + DSL + schema + cursor + error) always importable; `[ws]` adds `websockets`; `[admin]` adds admin | Mirrors cargo features; lean dep tree for HTTP-only users. Default install includes all. |
| Toolchain | `ruff format` + `ruff check` + `pyright` + `pytest`/`pytest-asyncio`; pre-commit (secret scan) | Per `~/.claude/guides/python.md`. |
| HTTP/WS libs | `httpx>=0.27` (async), `websockets>=13`, `pydantic>=2` | Modern async, broadly available. 10s default timeout (guide). |
| Auth | Machine bearer tokens for data/WS/HTTP; `auth_me`/`validate_session_token` helpers | Python apps are trusted machine clients; OAuth is browser-only. |
| Wire types | Re-declared byte-identical (fourth contract copy) with round-trip parity tests vs server/TS/Rust fixtures | Keeps the four-way contract honest. |

## Wire contract (fourth implementation)

The package re-declares these types as Pydantic v2 models whose `model_dump_json(
by_alias=True, exclude_none=...)` reproduces the server's wire bytes exactly. Every
model sets `model_config = ConfigDict(extra="forbid", populate_by_name=True)` — the
Pydantic equivalent of `deny_unknown_fields`. Fields whose wire key differs from the
Python name declare an explicit `alias`.

**Messages** — discriminator `"type"`, `to_camel` field names:

```
ClientMessage (union, discriminator="type", extra=forbid):
  auth{token, db} | subscribe{queryId, query} | unsubscribe{queryId}
  | mutate{mutId, idempotencyKey?, txn}
  | schedule{scheduleId, when, txn} | cancelSchedule{scheduleId, id}
  | pauseSchedule{scheduleId, id} | resumeSchedule{scheduleId, id} | listSchedules{scheduleId}
  | ping
ServerMessage (union, discriminator="type"):
  authOk{user} | authErr{error}
  | queryUpdate{queryId, result}
  | mutateOk{mutId, results[]} | mutateErr{mutId, error}
  | subscribeErr{queryId, error}
  | scheduleOk{scheduleId, id} | scheduleErr{scheduleId, error}
  | scheduleAck{scheduleId, ok, error?} | listSchedulesOk{scheduleId, schedules[]}
  | pong
AuthedUser: { kind: "user"|"machine", email?, name?, githubLogin?, githubId? }
ScheduleWhen: AfterMs{ms} | RunAt{ms} | Cron{expr}    # tag "type"; values "afterMs"/"runAt"/"cron"
ScheduleInfo: { id, kind, dueAt, cron?, status, lastError?, createdAt, firedCount }
```

**Query** — plain model, wire field names **snake_case** (so Python names match the
wire directly), all fields optional/defaulted:

```
Query: { table, get?, index?, eq[]?, gt?|gte?, lt?|lte?, order?("asc"|"desc"),
         take?(int, max 4096), unique?(bool), first?(bool), count?(bool),
         filter?: FilterExpr,
         search?: { index, query },
         vectorSearch?: { index, vector[], limit, filter? },     # camelCase key; `filter` is an eq-map {field: value} over the index's filterFields (NOT a FilterExpr)
         paginate?: { cursor?, numItems } }
```

`QueryResult` is **untagged** (the server serializes by shape; the client deserializes
by the terminal it issued): an object or `null` (`Doc`), an array (`Docs`), a bare
number (`Count`), or `{ docs[], nextCursor? }` (`Paginated`). The client maps these onto
`T | None`, `list[T]`, `int`, and `Paginated[T]` respectively, driven by the terminal
the caller used (mirrors Rust's `parse_result<T>`) — no ambiguous untagged union at the
API boundary.

**Transaction** — `{ steps: Step[] }`, max 256 steps. `Step` is discriminator `"op"`,
camelCase, `extra="forbid"`:

```
insert{table, doc} | patch{table, id, fields} | replace{table, id, doc}
| delete{table, id} | expectVersion{table, id, version:int}
| expectAbsent{table, index, eq[]} | upsert{table, index, eq[], insert, patch}
```

`doc`/`fields`/`insert`/`patch` are arbitrary JSON objects (`dict[str, Any]` / a Pydantic
`BaseModel` the client serializes). Per-step results (positional, aligned with `steps`):
`insert → {id}`, `upsert → {id, inserted}`, `patch/replace/delete/expectVersion/
expectAbsent → None`.

**FilterExpr** — predicate DSL, internally tagged by `"op"` (variant names **lowercase**):
`eq|neq|gt|gte|lt|lte|in` (leaf, `{field, value}` / `in` `{field, values[]}`) + `and|or`
(`{exprs[]}`). Composes with every terminal except `get`. (Distinct from
`VectorSearchQuery.filter`, which is an eq-map, not a FilterExpr.)

**Schema** — `SchemaDef = { tables: { name: { fields: { name: FieldType },
indexes?: [{ name, fields[], search?, vector? }], ownerField? } } }`. `FieldType` is
discriminator `"type"`, camelCase, `extra="forbid"`:

```
string | number | boolean | null | id{table} | literal{value} | optional{inner}
| union{variants[]} | array{element} | object{fields} | int64 | bytes | any
| record{value} | vector{dimensions}
```

`int64` and `Id` are JSON **strings** on the wire (never numbers) to stay exact across
the full `i64`/id range — typed as `str` (branded `Annotated[str, ...]`) on the client.

**Errors** — `{ code, message }`. `code` is `SCREAMING_SNAKE_CASE`:
`UNAUTHORIZED(401) | FORBIDDEN(403) | NOT_FOUND(404) | SCHEMA_VIOLATION(422) |
PRECONDITION_FAILED(409) | BAD_REQUEST(400) | INTERNAL(500)`. Same envelope on HTTP
bodies and inside WS error frames.

**Round-trip parity tests** assert the package's serialized JSON equals the
server/TS/Rust fixture JSON for a battery of messages, queries, transactions, schemas,
filters, search/vector queries, and schedules. This is the safety net for the four-way
contract.

## Package structure

`python-client/` — `uv`-managed PEP 621 package `par-rt-db`, `src/` layout:

```text
python-client/
  pyproject.toml          # PEP 621; requires-python = ">=3.12"; ruff + pyright + pytest
  .gitignore
  Makefile                # install/test/lint/fmt/typecheck/checkall/pre-commit/pre-commit-update
  .pre-commit-config.yaml # ruff + pyright + secret scan (adapt ~/Repos/parllama)
  README.md
  src/par_rt_db/
    __init__.py           # re-exports, extra-gating via import errors
    errors.py             # RtDbError + ErrorCode + retry_on_precondition
    wire.py               # ClientMessage/ServerMessage/AuthedUser/Schedule* (discriminator unions)
    schema.py             # FieldType (15), SchemaDef/TableDef/IndexDef/VectorIndexSpec, builders, t
    query.py              # Query, FilterExpr, SearchQuery, VectorSearchQuery, TableQuery builder, QueryResult parse
    mutation.py           # Transaction, Step (7 ops), StepResult, Mutation builder
    cursor.py             # encode_cursor/decode_cursor (base64 of JSON array)
    http_client.py        # RtDbHttpClient (async)                      [core]
    ws_client.py          # RtDbClient (async reactive WS)              [extra: ws]
    admin.py              # RtDbAdminClient (async)                     [extra: admin]
    in_memory.py          # InMemoryRtDbClient (fake; correct filter/search/vector)
  tests/
    test_wire_parity.py   # round-trip JSON == fixtures (four-way contract)
    test_dsl.py           # schema/query/mutation builder shapes
    test_query_result.py  # untagged QueryResult parsing
    test_cursor.py        # cursor codec
    test_errors.py        # error mapping + retry
    test_ws_routing.py    # WS routing/backoff/jitter/liveness (no socket)
    test_in_memory.py     # fake-client semantics
    test_http_integration.py   # opt-in: live server (RTDB_TEST_SERVER_URL + admin key)
    test_ws_integration.py     # opt-in: live server
    test_admin_integration.py  # opt-in: live server
    common.py             # shared harness (provision db, mint token, push schema)
```

The root `Makefile` gains a `python-client` target so `make checkall` runs
`server/` + `ts-client/` + `rust-client/` + `dashboard/` + `python-client/`. `make
python-client-install` runs `uv sync` (dev extras).

## DSL — schema / query / mutation (Pydantic)

```python
from par_rt_db import t, Schema, TableQuery, Mutation, Ft  # FieldType ctor namespace

# Schema → exact SchemaDef for push_schema
schema = (
    Schema.builder()
    .table("boxes", lambda tb: tb
        .field("owner_id", t.id("players"))
        .field("status", t.string())
        .field("fsroot", t.id("fsnodes"))
        .index("by_status", ["status"])
        .owner_field("owner_id"))
    .table("players", lambda tb: tb.field("email", t.string()))
    .build()
)

# Query → typed result (T is a user BaseModel)
active: list[Box] = await http.query("by_status").eq("active").take(50).collect(Box)
one: Box | None       = await http.query("by_status").eq("active").first(Box)
n: int                = await http.query("by_status").eq("active").count()
by_id: Box | None     = await http.get(Box, "0123...")
page: Paginated[Box]  = await http.query("by_status").eq("active").order("asc").paginate(Box, None, 100)
found: list[Box]      = await http.query("by_status").eq("active").filter(Ft.eq("status","active")).take(10).collect(Box)
hits: list[Box]       = await http.query_search("boxes_idx", "ransomware").take(10).collect(Box)
near: list[Box]       = await http.query_vector("embed_idx", vec, limit=10).collect(Box)

# Mutation → step results
res = await http.mutate(
    Mutation.builder()
    .insert("boxes", {"owner_id": pid, "status": "active"})
    .patch("boxes", box_id, {"status": "idle"})
    .expect_version("boxes", box_id, 7)
    .upsert("boxes", "by_owner", [pid], insert_doc, patch_doc)
    .build(),
    idempotency_key="retry-1",  # optional safe-retry key
)
```

`query(...)` returns a `TableQuery` that is terminal-aware; each terminal takes the
result model `T` (a `BaseModel`) and parses via `T.model_validate(...)`. Table and index
names are plain `str` (runtime). The builder enforces client-side only what is locally
knowable (range/terminal mutual exclusion mirrors the server's rules to fail fast);
everything schema-shaped is validated by the server.

## HTTP client — core (always available)

`RtDbHttpClient(url, db, token, *, timeout=10.0)` — `token` is a machine token (or user
session token); sent as `Authorization: Bearer <token>` on every call. `httpx.AsyncClient`.

| Method | Endpoint | Body | Returns |
|---|---|---|---|
| `query(...)` builder / `get(T, id)` | `POST /api/query` | `{ db, query }` | `T` (from `{ result }`) |
| `mutate(txn, *, idempotency_key=None)` | `POST /api/mutate` | `{ db, txn, idempotencyKey? }` | `list[StepResult]` |
| `mutate_with_retry(txn, *, max_attempts=5)` | `POST /api/mutate` | (rotates `idempotencyKey`) | `list[StepResult]` |
| `schedule / cancel / pause / resume / list_schedules` | `POST /api/schedule`, `POST /api/schedule/{id}/{cancel,pause,resume}`, `POST /api/schedules` | per op | `ScheduleInfo` / `list[ScheduleInfo]` |
| `upload(db, bytes, content_type)` / `delete_file` / `get_file_metadata` / `get_url` | `POST /api/storage/{db}` (raw body), `DELETE /api/storage/{db}/{id}`, `GET /api/storage/{db}/{id}/metadata`, `GET /storage/{id}` | — | `{ id }` / metadata / url |
| `auth_me(session_token)` | `GET /auth/me` | bearer = session | `AuthedUser`; machine tokens → 401 |
| `validate_session_token(session_token)` | `GET /auth/validate` | bearer = session | `bool` |

Non-2xx responses whose body parses as `{code, message}` become `RtDbError(code, message)`;
otherwise `RtDbError(INTERNAL, f"request failed with status {n}")`.

## Reactive WS client — `[ws]` extra

`RtDbClient(url, db, get_token, *, heartbeat=20.0, backoff_base=0.5, backoff_max=15.0)`
where `get_token` is an **async** callable `async () -> str | None` (async so a refreshed
token can be fetched on reconnect), called on every (re)open — mirroring TS `getToken`.
The URL origin's scheme is flipped `http→ws`/`https→wss` and `/sync` appended (plain
WebSocket; no subprotocol, no query-string auth). `websockets` async client.

Lifecycle mirrors the TS/Rust clients exactly:
- **Handshake**: on open, send `{type:"auth", token, db}` as the first frame; await
  `authOk{user}` / `authErr{error}`. Close code `4401` (auth failed) is **terminal** —
  no reconnect; `4400` (protocol violation / oversized / rate limit) reconnects.
- **Reconnect**: jittered exponential backoff (`min(max, base * 2**attempt) *
  (0.5 + rand*0.5)`), attempt reset on successful `authOk`; resubscribe every active query.
- **Keepalive**: send `{type:"ping"}` on `heartbeat`; if no `pong` within `2×heartbeat`,
  close with `4000` and reconnect.
- **Generation guard**: a monotonic counter bumped on every (re)open and on `close()`.
  Every `asyncio.Task` (token fetch, reconnect timer, keepalive timer) captures its
  generation and cancels itself if it has advanced — preventing stale callbacks from
  opening a duplicate socket.
- **Subscriptions**: `subscribe(query, T) -> Subscription[T]` — an async iterator
  (`async for value in sub`) plus `sub.current() -> T | None`; dedup by the canonical
  serialized query (one wire `subscribe` per unique shape, many iterators); `queryId`
  from a per-client counter (`sub-{n}`). The first `queryUpdate` delivers the first value
  (iterator yields nothing until then; `current()` is `None`). `unsubscribe` on the last
  iterator for a shape sends `{type:"unsubscribe"}`.
- **Mutations — at-most-once**: `mutate(txn, *, idempotency_key=None) ->
  Awaitable[list[StepResult]]`. `mutId` is a per-client counter (`mut-{n}`), pure reply
  correlation (never persisted); the optional `idempotency_key` is the wire
  `idempotencyKey` for safe retry. In-flight (sent, unacked) mutations are **rejected**
  on close (`RtDbError`, "connection closed before acknowledgment"); never-sent mutations
  are re-queued for the next connection. Never auto-resent.
- **Schedule ops** mirror `mutate`'s queue/reject contract.

## Admin control plane — `[admin]` extra

`RtDbAdminClient(url, admin_key)` — `Authorization: Bearer <admin_key>` on every call
(the configured `RTDB_ADMIN_KEY`, constant-time compared server-side). Covers the **full**
server admin surface (more than the TS client currently ships):

| Method | Endpoint | Returns |
|---|---|---|
| `create_db(name)` / `push_schema(db, schema)` / `list_dbs()` | `POST /admin/create-db`, `POST /admin/push-schema`, `GET /admin/dbs` | `()` / `()` / `list[str]` |
| `mint_token(db, name)` / `revoke_token(token_id)` / `list_tokens(db)` | `POST /admin/mint-token`, `POST /admin/revoke-token`, `GET /admin/tokens?db=` | `MintedToken` / `()` / `list[TokenInfo]` |
| `allowlist_add/remove(db, email)` / `allowlist_list(db)` | `POST /admin/allowlist`, `GET /admin/allowlist?db=` | `()` / `list[str]` |
| `admins_list/add/remove(email)` | `GET/POST/DELETE /admin/admins` | admin email allowlist |
| `get_schema(db)` / `db_stats(db)` | `GET /admin/dbs/{db}/schema`, `GET /admin/dbs/{db}/stats` | `SchemaDef` / stats |
| `admin_query(db, q)` / `admin_mutate(db, txn)` | `POST /admin/db/{db}/query`, `POST /admin/db/{db}/mutate` | owner-bypass read/write |
| `metrics()` / `get_config()` / `patch_config(patch)` | `GET /admin/metrics`, `GET /admin/config`, `PATCH /admin/config` | metrics / redacted config / `()` |
| `ops_recent()` | `GET /admin/ops/recent` | op feed |
| `export_db(db)` / `import_db(db, jsonl)` | `GET /admin/export-db?db=`, `POST /admin/import-db?db=` | JSONL `str` / `()` |

(`/admin/stream` is a WS; a `stream()` async iterator is included for parity, authing
via the `Sec-WebSocket-Protocol` subprotocol `rtdb-admin.<token>` like the dashboard.)

## In-memory test harness — `InMemoryRtDbClient`

A no-network, no-Postgres re-implementation of the server's query/txn/subscribe/owner
semantics for app unit tests (the Python analog of TS `InMemoryRtDbClient` /
Convex's `convex-test`). Covers `push_schema`, `query`/`mutate`/`subscribe` (reactive),
all 7 step ops, schedule lifecycle + a `tick()` timer hook, storage surface, idempotency
cache, atomic rollback on step failure, system fields, keyset pagination. **Critically,
`filter`/`search`/`vector_search` are implemented correctly** (predicate evaluator for
`FilterExpr`; in-memory text match for search; cosine similarity for vector) — fixing the
TS harness bug where these silently return unfiltered results. Genuinely-hard surfaces
that are deferred throw a clear `NotImplementedError`, never silently misbehave.

## Error handling

`RtDbError(Exception)` with `code: ErrorCode` and `message: str` is the single error
type across all modules. Constructed from HTTP non-2xx `{code,message}` bodies and from
WS error frames; `retry_on_precondition(fn, *, max_attempts)` retries a mutation on
`PRECONDITION_FAILED` (the `expectVersion`/`expectAbsent` optimistic-concurrency path),
mirroring the TS/Rust retry helper.

## Testing

- **`test_wire_parity.py`** — the four-way contract safety net: for each fixture (message,
  query, txn, schema, filter, search/vector, schedule), `model_dump_json(by_alias=True)`
  equals the canonical JSON, and `Model.model_validate_json(...)` round-trips. Fixtures
  shared with/compared against server + TS + Rust where available.
- **DSL/builder shape tests**, **`QueryResult` untagged parsing**, **cursor codec**,
  **error mapping + retry**, **WS routing/backoff/jitter/liveness** (no real socket),
  **in-memory semantics**.
- **Opt-in live integration** (`@pytest.mark.live`, gated on `RTDB_TEST_SERVER_URL` +
  `RTDB_ADMIN_KEY`, skipped otherwise): HTTP round-trip, admin control plane, WS
  subscribe + live update. Shared `common.py` provisions a unique `t<uuid>` db.
- `pytest-asyncio` for all async tests; default per-test timeout ~10s.

## Out of scope (v1)

- **React bindings** — JS-only.
- **Browser OAuth helpers** (`signInWithGitHub`/`signInWithGoogle` popups) — browser-only;
  Python apps use machine tokens + `auth_me`/`validate_session_token`.
- **Sync wrappers** — additive follow-on (Approach B); not needed for v1 parity.
- **Client-side optimistic updates (#12)** — shipped in the TS client; deferred in this
  Python v1 (as it is in the Rust client). Additive follow-on.
- **Pydantic-based schema codegen / compile-time table-index typing** — the TS client's
  static type inference has no clean Python analog; runtime string names + Pydantic result
  models is the v1 story (matches the Rust client's deliberate "hybrid" choice).

## Phasing

Implementation plan (written via the writing-plans skill after spec approval) will phase:
1. Package scaffold + `pyproject` + Makefile/pre-commit + wire types + parity tests.
2. Schema/query/mutation DSL + cursor + errors (+ DSL tests).
3. HTTP client + integration.
4. Reactive WS client + integration.
5. Admin client + integration.
6. In-memory harness.
7. README + `FEATURE_MATRIX.md` Python column + root `make checkall` wiring.

## Related

- Rust client design (template): `docs/superpowers/specs/2026-07-22-rust-client-design.md`.
- TS client: `ts-client/src/`. Rust client: `rust-client/src/`. Server protocol:
  `server/src/protocol.rs`.
- Client-completeness audit gaps (separate workstream): ts-admin ×10 endpoints, ts
  Google-OAuth + `/auth/me`, ts in-memory `filter` fix; rust optimistic-updates (#12),
  rust in-memory harness (#19).
