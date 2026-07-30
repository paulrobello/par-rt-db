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
| Mutation DSL (`Mutation` builder, `Transaction`, `StepResult`, 7 step ops) | shipped | `par_rt_db.mutation` |
| Cursor codec (`encode_cursor` / `decode_cursor`) | shipped | `par_rt_db.cursor` |
| Error model (`RtDbError`, `ErrorCode`, `retry_on_precondition`) | shipped | `par_rt_db.errors` |
| HTTP / admin / storage client (`RtDbHttpClient`, sync `httpx`) | shipped | `par_rt_db.http_client` (`[http]` extra) |
| Reactive WebSocket client (`RtDbClient`, `Subscription`) | shipped | `par_rt_db.ws_client` (`[ws]` extra) |

The DSL layer is feature-complete: every server query terminal
(`get`/`index`+`eq`/`gt`/`gte`/`lt`/`lte`/`order`/`take`/`unique`/`first`/`count`/
`filter`/`search`/`vector_search`/`paginate`) and every mutation step
(`insert`/`patch`/`replace`/`delete`/`expectVersion`/`expectAbsent`/`upsert`)
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

Requires Python ≥ 3.12 and `pydantic>=2.7`. The optional HTTP and WebSocket
client extras pull in `httpx` and `websockets` respectively:

```bash
pip install par-rt-db[http]    # sync HTTP client + admin control plane + storage
pip install par-rt-db[ws]      # reactive WebSocket client (live queries, WS mutations)
```

The DSL layer has no third-party dependency beyond pydantic.

Editable install for development:

```bash
cd python-client
uv sync                    # installs the dev group by default (pytest, ruff, pyright)
```

## Quick start

`import par_rt_db` exposes the public DSL surface used below. Examples assume
you have an HTTP client sending the wire payloads; the DSL produces
JSON-serializable `pydantic` models you can `model_dump(by_alias=True)` onto the
wire (the next plan ships the HTTP/WS clients that do this for you).

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

The builder caps at 256 steps (matching `server/src/txn.rs::MAX_STEPS`) and
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

# Full-text search over a declared search index.
search = TableQuery("items").search("by_name", "hello world").take(10).build()

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
        await client.mutate(Mutation.builder().insert("items", {"_id": "i1"}).build())
        async for value in sub:
            print(value)  # initial [] then [{"_id": "i1", ...}]
    await client.close()


asyncio.run(main())
```

`RtDbClient` multiplexes live subscriptions, at-most-once mutations, and
schedule ops over one `/sync` connection with auto-reconnect, re-auth, and
resubscribe. `get_token` is an async callable (it may refresh an OAuth token);
return `None` to pause reconnects. Each `subscribe()` returns a `Subscription`
that is both an async iterator (yields each new value) and exposes the latest
value via `.current()`. Install with `pip install par-rt-db[ws]`.

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
            }
        }
    }
)
# `model_dump(by_alias=True, exclude_none=True)` produces the wire shape to
# `POST /admin/schema?db=...` (or the TS-style `pushSchema` helper in the next plan).

cursor = encode_cursor(["kanban-board-1", 42, "i4"])  # opaque base64 string
sort_tuple = decode_cursor(cursor)  # round-trips back to the list
```

`t.string()` / `t.number()` / `t.vector(n)` / `t.id(table)` / `t.optional(inner)`
/ `t.union([...])` / `t.object({...})` are the field constructors; see
`par_rt_db/schema.py` for the full set of 15 variants.

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
