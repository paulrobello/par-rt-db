# par-rt-db (Python client)

Python client for [par-rt-db](..) — a port of the TypeScript SDK
([`../ts-client/`](../ts-client)) and Rust crate ([`../rust-client/`](../rust-client))
for server-side and automation apps. Speaks the server's declarative
query/transaction DSL; the wire layer is the **fourth** implementation of
par-rt-db's JSON contract (alongside `server/src/protocol.rs`,
`ts-client/src/protocol.ts`, and `rust-client/src/wire.rs`). No codegen: you
build a `SchemaDef` that serializes to the exact server `SchemaDef`, and
query/mutate results deserialize generically into your own `pydantic` models or
plain `dict`s.

Package name: `par-rt-db` → in Python, `import par_rt_db`.

## Status / features

| Surface | Status | Module |
| --- | --- | --- |
| Core wire types (`ClientMessage`, `ServerMessage`, `ScheduleWhen`, `FilterExpr`, …) | shipped | `par_rt_db.wire` |
| Schema DSL (`SchemaDef`, `TableDef`, `t` field constructors, `SchemaBuilder`) | shipped | `par_rt_db.schema` |
| Query DSL (`Query`, `TableQuery` builder, `Paginated`, `parse_result`) | shipped | `par_rt_db.query` |
| Mutation DSL (`Mutation` builder, `Transaction`, `StepResult`, 9 step ops) | shipped | `par_rt_db.mutation` |
| Cursor codec (`encode_cursor` / `decode_cursor`) | shipped | `par_rt_db.cursor` |
| Error model (`RtDbError`, `ErrorCode`, `retry_on_precondition`) | shipped | `par_rt_db.errors` |
| HTTP / admin / storage client (`RtDbHttpClient`, sync `httpx`) | shipped | `par_rt_db.http_client` (`[http]` extra) |
| Async HTTP / admin / storage client (`RtDbAsyncHttpClient`, `httpx.AsyncClient`) | shipped | `par_rt_db.aio_http_client` (`[aio]` extra) |
| Reactive WebSocket client (`RtDbClient`, `Subscription`) | shipped | `par_rt_db.ws_client` (`[ws]` extra) |
| Admin control plane (`RtDbAdminClient` — db/token/schema allowlist CRUD, webhooks, metrics, hot config, sessions, backups, snapshot export/import, schema preview, admin schedules CRUD, admin storage list/upload/delete, per-db anonymous-access toggle) | shipped | `par_rt_db.admin` (`[http]` extra; async twin `AsyncRtDbAdminClient` in the same module, `[aio]` extra) |
| In-memory test harness (`InMemoryRtDbClient` — no network, no Postgres) | shipped | `par_rt_db.in_memory` |
| Optimistic local-state updates (`OptimisticStore` for read-modify-write UI loops) | shipped | `par_rt_db.optimistic` |

The DSL layer is feature-complete: every server query terminal
(`get`/`index`+`eq`/`gt`/`gte`/`lt`/`lte`/`order`/`take`/`unique`/`first`/`count`/
`collect`/`distinct`/`aggregate`/`filter`/`search`/`vector_search`/`hybrid_search`/`paginate`)
and every mutation step
(`insert`/`patch`/`replace`/`delete`/`undelete` (FM-33)/`expectVersion`/
`expectAbsent`/`upsert`
per-id steps, the `patch_by_query`/`delete_by_query` bulk steps, plus the
`schedule(when, txn)`/`cancel_schedule(id)` scheduling steps and the
`start_workflow(spec)`/`cancel_workflow(id)` workflow steps (FM-29))
has a builder method that produces a wire-identical payload. Pydantic v2
`extra="forbid"` mirrors the server's `deny_unknown_fields` on every variant.

## Install

```bash
pip install par-rt-db
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add par-rt-db
```

Requires Python ≥ 3.12 (see `pyproject.toml` for the supported `pydantic` and, per extra, `httpx`/`websockets` versions). The optional HTTP and WebSocket
client extras pull in `httpx` and `websockets` respectively:

```bash
pip install par-rt-db[http]    # sync HTTP client + admin control plane + storage
pip install par-rt-db[aio]     # async HTTP/admin/storage client (httpx.AsyncClient)
pip install par-rt-db[ws]      # reactive WebSocket client (live queries, WS mutations)
```

The DSL layer has no third-party dependency beyond pydantic.

Editable install for development:

```bash
cd python-client
uv sync                    # installs the dev group by default (pytest, ruff, pyright)
```

## Quick start

`import par_rt_db` exposes the public DSL surface used below. The DSL produces
JSON-serializable `pydantic` models you can `model_dump(by_alias=True)` onto the
wire. For live use, install the sync HTTP/admin/storage surface
(`pip install par-rt-db[http]`), the async twin (`pip install par-rt-db[aio]`),
or the reactive WebSocket client (`pip install par-rt-db[ws]`) — see the
respective sections below.

### Atomic multi-step transaction

```python
from par_rt_db import Mutation

# A serializable three-step transaction: insert + patch + delete, executed
# atomically by the server's single-writer committer. One StepResult per step.
txn = (
    Mutation.builder()
    .insert("items", {"name": "x", "n": 1})
    .patch("items", "i1", {"n": 2})
    .delete("items", "i2")
    .build()
)

# Wire payload to send over HTTP (`POST /api/mutate`) or WS (`mutate` frame):
payload = txn.model_dump(by_alias=True, exclude_none=True)
# => {"steps": [{"op": "insert", "table": "items", "doc": {...}}, ...]}
```

The builder caps at 1024 steps (matching `server/src/txn.rs::MAX_STEPS`) and
raises `ValueError` eagerly so an over-cap transaction never reaches the wire.
Add `expect_version` / `expect_absent` preconditions for optimistic-concurrency
patterns; combine with `retry_on_precondition` from `par_rt_db.errors` for
read-modify-write loops.

### Query + subscribe

```python
from par_rt_db import TableQuery

# Ordered scan into a page of 10 docs via the `by_n` index. `table` is the only
# required field; every other wire field is optional and omitted when None.
query = TableQuery("items").with_index("by_n").eq("kanban-board-1").order("asc").take(10).build()
payload = query.model_dump(by_alias=True, exclude_none=True)
# => {"table": "items", "index": "by_n", "eq": ["kanban-board-1"],
#     "order": "asc", "take": 10}

# Point read (one terminal, mutually exclusive with take/unique/first/count/paginate).
point = TableQuery("items").get("i1").build()

# Full-text search over a declared search index. `mode="trgm"` switches to
# case-insensitive substring/autocomplete matching over the index's text fields
# (FM-30); an omitted `mode` (or `"tsquery"`) is full-text — and omits `mode`
# from the wire entirely, so existing requests stay byte-identical. The query
# text honors web search operators (FM-31): quoted phrases require adjacency
# (`"exact phrase"`), the bare word `or` unions, `-term` excludes. `snippet=True`
# adds a server-rendered `_searchSnippet` (`<mark>`-highlighted fragment) to
# each hit — tsquery mode only.
search = TableQuery("items").search("by_name", "hello world").take(10).build()
autocomplete = TableQuery("items").search("by_name", "conv", mode="trgm").take(10).build()
phrase = TableQuery("items").search("by_name", '"release notes" -draft', snippet=True).build()

# Vector similarity over a pgvector index (embeddings are client-supplied).
vs = TableQuery("docs").vector_search("by_embedding", [0.1, 0.2, ...], limit=5).build()
```

Over the reactive WebSocket (`/sync`), send a `subscribe` frame carrying the
serialized query; the server pushes a `queryUpdate` on every committed write
that touches the table. Use `parse_result(model, terminal, payload)` from
`par_rt_db.query` to deserialize the untagged `QueryResult` into a typed
`list[model]`, `model | None`, `int`, or `Paginated[model]`.

### Reactive WebSocket (`[ws]` extra)

```python
import asyncio
from par_rt_db import Mutation, TableQuery
from par_rt_db.ws_client import RtDbClient


async def main() -> None:
    async def get_token() -> str | None:
        return _token()  # your per-db machine token or OAuth session token

    client = RtDbClient("wss://rtdb.pardev.net", "mydb", get_token=get_token)
    await client.connect()
    async with client.subscribe(TableQuery("items").collect()) as sub:
        # ``_id`` is server-managed (reserved); insert user fields only.
        await client.mutate(Mutation.builder().insert("items", {"name": "widget", "n": 1}).build())
        async for value in sub:
            print(value)  # initial [] then [{"_id": "<server-assigned>", "name": "widget", "n": 1}]
    await client.close()


asyncio.run(main())
```

`RtDbClient` multiplexes live subscriptions, at-most-once mutations, and
schedule ops over one `/sync` connection with auto-reconnect, re-auth, and
resubscribe. `get_token` is an async callable (it may refresh an OAuth token);
return `None` to pause reconnects. Each `subscribe()` returns a `Subscription`
that is both an async iterator (yields each new value) and exposes the latest
value via `.current()`. Install with `pip install par-rt-db[ws]`.

### Async HTTP / admin / storage (`[aio]` extra)

`RtDbAsyncHttpClient` is a one-to-one async mirror of `RtDbHttpClient` over
`httpx.AsyncClient` — every public method is a coroutine with the same name,
arguments, and return types as the sync client. Use it from async frameworks
(FastAPI, asyncio apps) instead of thread-wrapping the sync client.

```python
import asyncio

from par_rt_db import Mutation, RtDbAsyncHttpClient, TableQuery


async def main() -> None:
    # Same `(url, db, token)` shape as the sync client; `async with` closes it.
    async with RtDbAsyncHttpClient("https://rtdb.pardev.net", "mydb", "<machine-token>") as client:
        rows = await client.run(TableQuery("items").collect())
        await client.mutate(
            Mutation.builder().insert("items", {"_id": "i1", "name": "widget", "n": 1}).build()
        )


asyncio.run(main())
```

Install with `pip install par-rt-db[aio]` (same `httpx` pin as `[http]`; see `pyproject.toml`).

### Schemas and cursors

```python
from par_rt_db import SchemaDef, t, encode_cursor, decode_cursor

schema = SchemaDef.model_validate(
    {
        "tables": {
            "items": {
                "fields": {
                    "name": t.string(),
                    "n": t.number(),
                    "embedding": t.vector(384),
                    "owner": t.id("users"),
                },
                "indexes": [
                    {"name": "by_n", "fields": ["n"]},
                    {"name": "by_name", "fields": ["name"], "search": True},
                    {
                        "name": "by_embedding",
                        "fields": ["embedding"],
                        "vector": {"dimensions": 384, "filterFields": ["owner"]},
                    },
                ],
                "ownerField": "owner",
                # Field-level defaults (FM-32): stamped onto a NEW document
                # that omits the key (insert/replace/upsert-insert only;
                # patch never re-applies).
                "defaults": {"n": 0},
            }
        }
    }
)
# `model_dump(by_alias=True, exclude_none=True)` produces the wire shape to
# `POST /admin/push-schema` (or use `admin.push_schema(db, schema)` from the
# admin client — `pip install par-rt-db[http]`).

cursor = encode_cursor(["kanban-board-1", 42, "i4"])  # opaque base64 string
sort_tuple = decode_cursor(cursor)  # round-trips back to the list
```

`t.string()` / `t.number()` / `t.vector(n)` / `t.id(table)` / `t.optional(inner)`
/ `t.union([...])` / `t.object({...})` are the field constructors; see
`par_rt_db/schema.py` for the full set of 15 variants.

### Schema migration (`[http]` extra)

Destructive/type-changing schema transformations are a deliberate admin operation,
separate from the additive schema push. Build a `Migration` and apply (or preview)
it via the HTTP admin client — `POST /admin/db/{db}/migrate` runs the directives
transactionally inside the server's committer, so live queries, the op feed,
audit, and webhooks all fire.

```python
from par_rt_db import Cast, Migration
from par_rt_db.schema import t
from par_rt_db.admin import RtDbAdminClient

admin = RtDbAdminClient("https://rtdb.pardev.net", ADMIN_KEY)
result = admin.migrate_schema(
    "kanban",
    Migration.builder()
    .rename_field("items", "title", "summary")
    .change_type("items", "order", t.string(), Cast.TO_STRING, default="0")
    .set_default("items", "status", "backlog")
    .dry_run()  # preview first — returns the report + derived schema
    .build()
    .directives,
    dry_run=True,
)
# re-run with dry_run=False to apply
```

`change_type` takes a closed `Cast` (`TO_STRING`/`TO_NUMBER`/`TO_INT64`/
`TO_BOOLEAN`); the optional `default` substitutes for un-coercible rows (without
it a single bad value rolls the whole migrate back atomically). `eval_expr` is the
scoped raw-SQL escape hatch (one table's `doc` jsonb, no joins/DDL). See
[`docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md`](../docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md).

### Durable workflows (FM-29)

A named spec of steps — each an ordinary dumped `Transaction` plus optional
`StepRetry` and `sleep_before_ms` — the server advances durably (at-least-once
per step; a step that exhausts its retries fails the run). The sync and async
HTTP clients return the new run id; the reactive client returns `WorkflowInfo`.

```python
from par_rt_db import Mutation
from par_rt_db.wire import StepRetry, WorkflowSpec, WorkflowStepSpec
from par_rt_db.ws_client import RtDbClient  # or RtDbHttpClient from par_rt_db.http_client

db = RtDbClient("wss://rtdb.pardev.net", "mydb", get_token=get_token)  # as in the reactive example above

txn = Mutation.builder().insert("work_items", {"title": "welcome"}).build()
txn2 = Mutation.builder().insert("work_items", {"title": "follow-up"}).build()
spec = WorkflowSpec(
    name="onboard",
    steps=[
        WorkflowStepSpec(txn=txn.model_dump(by_alias=True)),
        WorkflowStepSpec(
            txn=txn2.model_dump(by_alias=True),
            retry=StepRetry(max_attempts=5),
            sleep_before_ms=60_000,
        ),
    ],
)
run_id: str = db.start_workflow(spec)  # reactive client: WorkflowInfo
db.cancel_workflow(run_id)  # False for a missing/terminal run
runs = db.list_workflows("running")  # list[WorkflowInfo], newest first

# …or start one atomically inside a txn:
Mutation.builder().insert("users", {"name": "a"}).start_workflow(spec).build()
```

Steps fire as the system principal (a scoped machine token is confined at
submit time), so write idempotent step txns. The admin client adds
`admin_list_workflows`/`admin_start_workflow`/`admin_get_workflow`/
`admin_cancel_workflow`/`admin_delete_workflow` (sync + async) over the
`/admin/db/{db}/workflows` routes, and the in-memory harness models the
engine (spec validation + `tick()` advance) so workflow flows are testable
with no network.

### Cascade delete + soft delete (FM-33)

A table field declared as `t.id(table, on_delete=...)` — legal only on a
top-level id (or `optional` id) field with a single-field, non-unique,
non-partial btree index on it — makes the server expand that reference when the
referenced row hard-deletes: `cascade` deletes the children (recursively),
`restrict` rejects the delete with `CONFLICT` while live children exist, and
`setNull` (requires the optional wrapper) removes the child's field key. A
table built with `.soft_delete()` turns its own deletes into a `deleted_at`
tombstone: the row disappears from every read, write lookup, and unique-index
enforcement (the unique key frees up for re-insert), and `undelete` restores it
(idempotent on a live row, `NOT_FOUND` when absent, `BAD_REQUEST` on a table
without `softDelete`). A soft delete never triggers a cascade; the TTL reaper
always hard-deletes.

```python
from par_rt_db import Mutation
from par_rt_db.schema import Schema, t

schema = (
    Schema.builder()
    .table("users", lambda tb: tb.field("name", t.string()))
    .table(
        "posts",
        lambda tb: (
            tb.field("title", t.string())
            .field("authorId", t.id("users", on_delete="cascade"))
            .index("by_author", ["authorId"])
        ),
    )
    .table(
        "comments",
        lambda tb: (
            tb.field("body", t.string())
            .field("postId", t.id("posts", on_delete="cascade"))
            .index("by_post", ["postId"])
            .soft_delete()
        ),
    )
    .build()
)

# Deleting the user cascades: posts hard-delete, comments soft-delete (stamped).
db.push_schema(schema)
db.mutate(Mutation.builder().delete("users", user_id).build())
db.mutate(Mutation.builder().undelete("comments", comment_id).build())  # restore
```

On the wire, `onDelete` rides the id variant (omitted when unset) and
`softDelete` rides the table (omitted when false); the undelete step is
`{"op": "undelete", "table": ..., "id": ...}`. Cascades are bounded
(`MAX_CASCADE_ROWS` per initiating step, `CONFLICT` past it), cycle-guarded,
and atomic with the txn. The in-memory harness mirrors all of it — including
push-time validation with the server's exact messages — in
`tests/test_cascade.py`.

## Errors

Every failure is `RtDbError { code, message }` with `ErrorCode` matching the
server's `SCREAMING_SNAKE_CASE` codes (`UNAUTHORIZED`, `PRECONDITION_FAILED`,
`SCHEMA_VIOLATION`, …). `RtDbError.from_http(status, body)` parses the wire
envelope; `RtDbError.from_envelope(dict)` builds one from an already-parsed
body; `RtDbError.status_code` returns the mapped HTTP status.
`retry_on_precondition` (in `par_rt_db.errors`) is a bounded async helper for
read-modify-write loops (`expect_version` / `expect_absent` + retry).

## Wire contract

`par_rt_db/wire.py` is the **fourth** implementation of par-rt-db's protocol
contract (alongside the server, the TS SDK, and the Rust crate). They must stay
byte-identical — same discriminator tags (`type` / `op`), same camelCase field
aliases, same omit-when-absent rules. Changing the wire format on any side is a
breaking change unless mirrored across all four. See
[`../CLAUDE.md`](../CLAUDE.md) and the design spec
[`../docs/superpowers/specs/2026-07-25-python-client-design.md`](../docs/superpowers/specs/2026-07-25-python-client-design.md).
`tests/test_wire_parity.py` is the cross-client oracle — it pins the Python
shapes against the same fixtures the server and the other clients validate.

## Develop

```sh
uv sync                           # install dev deps (pytest, ruff, pyright)
uv run pytest                     # full suite (no server, no Postgres needed)
uv run ruff check src tests       # lint
uv run ruff format src tests      # format
uv run pyright                    # type check
```

Single test by name: `uv run pytest tests/test_mutation.py -k insert`.

From the repo root, `make python-client-test` runs the suite as part of the
repo-wide `make checkall` gate.
