# Python Client Reactive WebSocket Surface — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the reactive `/sync` WebSocket client for the Python SDK — async `RtDbClient` with live subscriptions, at-most-once mutations, schedule ops, reconnect/backoff, and heartbeat — at parity with the TS and Rust clients.

**Architecture:** A single background `asyncio.Task` ("the driver") owns the socket and runs the recv loop, epoch/reconnect control, and heartbeat. All sends are serialized through one `asyncio.Lock`. `subscribe()` returns a sync `Subscription` (async iterator + `.current()` + `.error()`). Mutations/schedule ops are at-most-once: queued while disconnected, rejected ("connection closed before acknowledgment") only if the socket drops after the frame is sent but before the ack. The whole client is unit-testable with **no socket and no real timers** via dependency injection: an injectable `connect` factory (fake transport), and injectable `now`/`random`/`sleep` callables.

**Tech Stack:** Python 3.12+, asyncio, `websockets>=13` (the `[ws]` extra), Pydantic v2, pytest + pytest-asyncio (`asyncio_mode="auto"`).

## Global Constraints

- **Wire contract is load-bearing and byte-identical** across `server/src/protocol.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, and `python-client/src/par_rt_db/wire.py`. Reuse the existing `ClientMessage`/`ServerMessage` pydantic models from `wire.py` — do **not** redefine frame shapes. Casing is camelCase on the wire (handled by the `_Camel` base + `to_camel`).
- **Single error type:** every failure is `RtDbError(code, message)` from `errors.py`. WS error frames (`authErr`/`mutateErr`/`subscribeErr`/`scheduleErr`) carry the `{code, message}` envelope — build via `RtDbError.from_envelope(env.model_dump())` or `RtDbError(ErrorCode(env.code), env.message)`.
- **No `websockets` import at module top** of `ws_client.py` — import it lazily inside the default `connect` factory (mirrors how `http_client.py` lazily imports `httpx`), so importing the module / running fake-transport tests does not require the extra. Only constructing with the default factory requires `[ws]`.
- **No `unwrap`/bare `except`** — pyright strict-ish (`reportMissingImports="error"`) and `ruff` (`E,F,I,UP,B,SIM`, line-length 100) must stay clean. Zero lint warnings.
- **Auth is an in-band first JSON frame** `{type:"auth",token,db}` — never a subprotocol, header, or query param (matches both reference clients and the server).
- **Reactive model = current-value-that-updates** (the spec's choice): `async for value in sub` + `sub.current()`. A failed subscription (`subscribeErr`) surfaces as `RtDbError` raised from the iterator and exposed via `sub.error()`, and the failed shape is removed (not resent on reconnect).
- **Verification gate (every task):** `make python-client-checkall` (runs `ruff format --check` → `ruff check` → `pyright` → `pytest -q` in `python-client/`). First-time setup: `make python-client-install` (runs `uv sync --all-extras`, which installs `websockets` + `httpx` + the dev group).
- **Commit after every task** (atomic, conventional message). The repo is trunk-based — commit directly on `main`.

## File Structure

- **Create** `python-client/src/par_rt_db/ws_client.py` — the async reactive client (one cohesive module; mirrors `http_client.py`'s ~400-line shape).
- **Modify** `python-client/src/par_rt_db/query.py` — relocate `_dump_query` and `_terminal_of` here from `http_client.py` (their natural home; now two consumers).
- **Modify** `python-client/src/par_rt_db/http_client.py` — import the two relocated helpers from `query` instead of defining them.
- **Modify** `python-client/src/par_rt_db/__init__.py` — lazy `__getattr__` branch for `RtDbClient` + `Subscription` (keep `websockets` optional).
- **Modify** `python-client/pyproject.toml` — register the `live` pytest marker.
- **Create** `python-client/tests/test_ws_routing.py` — no-socket unit tests (the bulk of coverage).
- **Create** `python-client/tests/test_ws_integration.py` — opt-in live-server test (`@pytest.mark.live`, env-gated).
- **Modify** `FEATURE_MATRIX.md` — flip the Python reactive row(s) ❌→✅.
- **Modify** `python-client/README.md` — document the `[ws]` extra + a usage snippet.

---

## Task 1: Pure helpers + DRY relocation of query helpers

**Files:**
- Create: `python-client/src/par_rt_db/ws_client.py`
- Modify: `python-client/src/par_rt_db/query.py` (move `_dump_query`, `_terminal_of` here)
- Modify: `python-client/src/par_rt_db/http_client.py:87-123` (delete the two helpers, import from `query`)
- Test: `python-client/tests/test_ws_routing.py` (new), `python-client/tests/test_query.py` (add direct tests)

**Interfaces:**
- Produces: `_sync_url(url: str) -> str`, `_canonical_key(query_dict: dict[str, Any]) -> str`, `_backoff_delay(attempt: int, base: float, max_delay: float, rand: float) -> float` in `ws_client.py`; and `_dump_query`, `_terminal_of` now importable from `par_rt_db.query`.

- [ ] **Step 1: Write the failing tests**

Create `python-client/tests/test_ws_routing.py`:
```python
"""No-socket unit tests for the reactive WS client (framing, backoff, dedup, routing)."""

from par_rt_db.ws_client import _backoff_delay, _canonical_key, _sync_url


def test_sync_url_flips_scheme_and_appends_sync():
    assert _sync_url("http://localhost:8300") == "ws://localhost:8300/sync"
    assert _sync_url("https://rtdb.example.com") == "wss://rtdb.example.com/sync"
    assert _sync_url("ws://localhost:8300/") == "ws://localhost:8300/sync"
    assert _sync_url("wss://rtdb.example.com///") == "wss://rtdb.example.com/sync"


def test_canonical_key_is_order_independent():
    a = {"table": "items", "index": "by_x", "eq": [1]}
    b = {"index": "by_x", "eq": [1], "table": "items"}
    assert _canonical_key(a) == _canonical_key(b)


def test_canonical_key_distinguishes_different_shapes():
    assert _canonical_key({"table": "a"}) != _canonical_key({"table": "b"})


def test_backoff_delay_is_bounded_and_jittered():
    base, top = 0.5, 15.0
    # rand = 0.5 -> exactly half of the raw cap.
    assert _backoff_delay(0, base, top, 0.5) == 0.25
    # attempt grows exponentially until capped at `top`; jitter in [0.5, 1.0] of raw.
    raw = min(top, base * (2 ** 5))
    lo, hi = raw * 0.5, raw * 1.0
    assert lo <= _backoff_delay(5, base, top, 0.0) <= hi
    # never exceeds `top`.
    assert _backoff_delay(50, base, top, 1.0) <= top
```

Add to `python-client/tests/test_query.py` (direct tests for the relocated helpers):
```python
from par_rt_db.query import Query, TableQuery, _dump_query, _terminal_of


def test_dump_query_serializes_tablequery_to_wire_dict():
    q = TableQuery("items").with_index("by_x", [1]).take(5)
    d = _dump_query(q)
    assert d["table"] == "items"
    assert d["index"] == "by_x"
    assert d["eq"] == [1]
    assert d["take"] == 5


def test_terminal_of_infers_collect_by_default():
    assert _terminal_of(Query(table="items")) == "collect"
    assert _terminal_of(Query(table="items", count=True)) == "count"
    assert _terminal_of(Query(table="items", get="i1")) == "get"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py tests/test_query.py -q`
Expected: FAIL — `ImportError: cannot import name '_backoff_delay' ...` / `_dump_query` not in `query`.

- [ ] **Step 3: Relocate the query helpers**

In `python-client/src/par_rt_db/query.py`, add (move verbatim from `http_client.py`):
```python
def _dump_query(q: "Query | TableQuery") -> dict[str, Any]:
    """Serialize a Query (or TableQuery) to its wire-shaped dict."""
    built = q.build() if isinstance(q, TableQuery) else q
    return built.model_dump(by_alias=True, mode="json")


def _terminal_of(q: "Query") -> str:
    """Infer the parse_result terminal from a built Query."""
    if q.get is not None:
        return "get"
    if q.count:
        return "count"
    if q.first:
        return "first"
    if q.unique:
        return "unique"
    if q.distinct:
        return "distinct"
    if q.aggregate is not None:
        return "aggregateGroups" if q.aggregate.group_by else "aggregate"
    if q.paginate is not None:
        return "paginate"
    return "collect"
```
In `python-client/src/par_rt_db/http_client.py`, delete the local `_dump_query` (lines ~87-96) and `_terminal_of` (lines ~99-123), and add to the existing `from .query import (...)` block: `_dump_query, _terminal_of`.

- [ ] **Step 4: Create `ws_client.py` with the three pure helpers**

Create `python-client/src/par_rt_db/ws_client.py`:
```python
"""Reactive WebSocket client for par-rt-db (the ``[ws]`` extra).

Async client over the ``/sync`` endpoint: one multiplexed connection, live
subscriptions (``async for value in client.subscribe(query)``), at-most-once
mutations, and schedule ops. Mirrors ``ts-client/src/client.ts`` and
``rust-client/src/ws.rs``. ``websockets`` is imported lazily inside the default
``connect`` factory so this module imports without the ``[ws]`` extra installed.
"""

from __future__ import annotations

import json
import random as _random
import time
from typing import Any


def _sync_url(url: str) -> str:
    """Flip http(s)→ws(s), strip trailing slashes, append ``/sync``."""
    u = url.strip()
    if u.startswith("https://"):
        u = "wss://" + u[len("https://"):]
    elif u.startswith("http://"):
        u = "ws://" + u[len("http://"):]
    return u.rstrip("/") + "/sync"


def _canonical_key(query_dict: dict[str, Any]) -> str:
    """Stable dedup key for a query's wire dict (order-independent)."""
    return json.dumps(query_dict, sort_keys=True, separators=(",", ":"))


def _backoff_delay(attempt: int, base: float, max_delay: float, rand: float) -> float:
    """Jittered exponential backoff: ``min(max, base * 2**attempt) * (0.5 + rand*0.5)``."""
    raw = min(max_delay, base * (2 ** attempt))
    return raw * (0.5 + rand * 0.5)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py tests/test_query.py tests/test_http_client.py -q`
Expected: PASS (the http_client tests still pass — they exercise the relocated helpers via `run`/`mutate`).

- [ ] **Step 6: Lint + typecheck**

Run: `cd python-client && uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add python-client/src/par_rt_db/ws_client.py python-client/src/par_rt_db/query.py python-client/src/par_rt_db/http_client.py python-client/tests/test_ws_routing.py python-client/tests/test_query.py
git commit -m "feat(python-client): add ws_client pure helpers; relocate _dump_query/_terminal_of to query"
```

---

## Task 2: Connection core — driver, handshake, reconnect, heartbeat

**Files:**
- Modify: `python-client/src/par_rt_db/ws_client.py`
- Test: `python-client/tests/test_ws_routing.py` (add a fake-transport harness + connection tests)

**Interfaces:**
- Consumes (from Task 1): `_sync_url`, `_backoff_delay`.
- Consumes (from `wire.py`): the `_Client*` / `_Server*` models and the `ClientMessage`/`ServerMessage` aliases; `AuthedUser`.
- Consumes (from `errors.py`): `RtDbError`, `ErrorCode`.
- Produces: `ConnectionState` (StrEnum), `ClientStatus` (dataclass), `Connection` (typing.Protocol), `_PeerClosed`, the default `_default_connect` factory, and `RtDbClient` with `__init__`, `async connect()`, `async close()`, `status()`. The driver's server-message dispatch (`_dispatch(msg)`) is defined here with forward-compatible handlers: `_on_query_update`, `_on_subscribe_err`, `_on_mutate_ok`, `_on_mutate_err`, and the schedule-ack handlers — all implemented as **no-ops when their target map entry is absent** (the maps are populated by Tasks 3–4).

### The fake-transport test harness (define in `test_ws_routing.py`)

```python
import asyncio
import pytest
from par_rt_db.ws_client import RtDbClient, ConnectionState, _PeerClosed


class FakeConn:
    """Stand-in Connection: records sent frames, lets tests feed inbound frames."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self._inbox: asyncio.Queue[str] = asyncio.Queue()
        self.close_code: int | None = None

    async def send(self, data: str) -> None:
        self.sent.append(data)

    async def recv(self) -> str:
        item = await self._inbox.get()
        if item is None:
            raise _PeerClosed(self.close_code or 1000, "")
        return item

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_code = code
        await self._inbox.put(None)

    async def deliver(self, frame: str) -> None:
        await self._inbox.put(frame)


class FakeClock:
    """Virtual clock + sleep for deterministic timing tests."""

    def __init__(self) -> None:
        self.now = 0.0
        self.advancing: list[asyncio.Future] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self.advancing.append((seconds, fut))
        await fut  # the test advances the clock to wake this


def make_client(tmp_conn, *, clock=None, rand=0.5, **kw):
    clock = clock or FakeClock()
    return RtDbClient(
        "http://x", "db",
        get_token=lambda: _const("tok"),
        connect=lambda url: _ready(tmp_conn),
        now=clock.time,
        random=lambda: rand,
        sleep=clock.sleep,
        heartbeat=20.0,
        **kw,
    ), clock


async def _const(v):
    return v


async def _ready(conn):
    return conn
```

The driver **must** call the injected `sleep`/`now`/`random` (never the stdlib ones directly). The default `__init__` binds `now=time.monotonic`, `random=_random.random`, `sleep=asyncio.sleep`.

- [ ] **Step 1: Write the failing connection tests**

Add to `test_ws_routing.py`:
```python
async def test_connect_sends_auth_then_marks_connected():
    conn = FakeConn()
    client, _ = make_client(conn)
    await client.connect()
    await _drain()  # let the driver run
    assert json.loads(conn.sent[0]) == {"type": "auth", "token": "tok", "db": "db"}
    assert client.status().state is ConnectionState.CONNECTING
    await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    assert client.status().state is ConnectionState.CONNECTED
    assert client.status().user.kind == "machine"
    await client.close()


async def test_autherr_is_terminal_no_reconnect():
    conn = FakeConn()
    client, clock = make_client(conn)
    await client.connect()
    await _drain()
    await conn.deliver('{"type":"authErr","error":{"code":"UNAUTHORIZED","message":"no"}}')
    await _drain()
    assert client.status().state is ConnectionState.IDLE
    # No backoff sleep was scheduled (terminal).
    assert clock.advancing == []
    await client.close()


async def test_reconnectable_close_schedules_backoff():
    conn = FakeConn()
    client, clock = make_client(conn, rand=0.5)
    await client.connect()
    await _drain()
    await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    # Peer drops with a non-auth code (e.g. 4000).
    conn.close_code = 4000
    await conn._inbox.put(None)
    await _drain()
    assert client.status().state is ConnectionState.RECONNECTING
    assert len(clock.advancing) >= 1  # a backoff sleep is pending


async def test_pong_resets_liveness():
    conn = FakeConn()
    client, _ = make_client(conn, heartbeat=0.05)
    await client.connect()
    await _drain()
    await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    await conn.deliver('{"type":"pong"}')
    await _drain()
    # A ping frame is eventually sent on the heartbeat interval.
    assert any('"type":"ping"' in f for f in conn.sent)
    await client.close()
```
Add a tiny helper at top of the test file:
```python
import json

async def _drain():
    """Yield to the event loop so the driver task makes progress."""
    for _ in range(5):
        await asyncio.sleep(0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py -q`
Expected: FAIL — `RtDbClient` / `ConnectionState` not defined.

- [ ] **Step 3: Implement the connection core**

Add to `ws_client.py` (after the helpers). Key pieces:

```python
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from pydantic import TypeAdapter

from .errors import ErrorCode, RtDbError
from .wire import AuthedUser, ClientMessage, ServerMessage

_SERVER = TypeAdapter(ServerMessage)


class ConnectionState(StrEnum):
    IDLE = "idle"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"


@dataclass
class ClientStatus:
    state: ConnectionState
    user: AuthedUser | None = None


class Connection(Protocol):
    async def send(self, data: str) -> None: ...
    async def recv(self) -> str: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class _PeerClosed(Exception):
    """Translated socket-close: carries the peer's close code."""
    def __init__(self, code: int, reason: str) -> None:
        super().__init__(f"peer closed: {code} {reason}")
        self.code = code
        self.reason = reason


_AUTH_FAILED_CODE = 4401
_AUTH_DEADLINE = 15.0


class RtDbClient:
    def __init__(
        self,
        url: str,
        db: str,
        get_token: Callable[[], Awaitable[str | None]],
        *,
        heartbeat: float = 20.0,
        backoff_base: float = 0.5,
        backoff_max: float = 15.0,
        connect: Callable[[str], Awaitable[Connection]] | None = None,
        now: Callable[[], float] = time.monotonic,
        random: Callable[[], float] = _random.random,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._url = _sync_url(url)
        self._db = db
        self._get_token = get_token
        self._heartbeat = heartbeat
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._connect = connect or _default_connect
        self._now = now
        self._random = random
        self._sleep = sleep

        self._state = ConnectionState.IDLE
        self._user: AuthedUser | None = None
        self._generation = 0          # bumped on every (re)open and on close()
        self._closed = False
        self._attempt = 0
        self._task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()
        self._ws: Connection | None = None
        self._last_pong = 0.0
        # Populated by Tasks 3-4:
        self._subs_by_key: dict[str, Any] = {}
        self._subs_by_id: dict[str, Any] = {}
        self._counter = 0
        self._pending_mut: dict[str, Any] = {}
        self._pending_sched: dict[str, Any] = {}

    def status(self) -> ClientStatus:
        return ClientStatus(self._state, self._user)

    async def connect(self) -> None:
        """Start (or resume) the driver. Idempotent."""
        if self._task is None:
            self._closed = False
            self._task = asyncio.create_task(self._drive())

    async def close(self) -> None:
        self._closed = True
        self._generation += 1
        self._ws = None
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        self._set_state(ConnectionState.CLOSED)
        self._reject_all_pending("client closed")

    # --- driver ---------------------------------------------------------

    async def _drive(self) -> None:
        while not self._closed:
            token = await self._get_token()
            if self._closed:
                return
            if token is None:
                self._set_state(ConnectionState.IDLE)
                await self._sleep(0)  # park; a connect()/set-token poke resumes via generation
                continue
            gen = self._generation
            await self._epoch(token, gen)

    async def _epoch(self, token: str, gen: int) -> None:
        self._set_state(ConnectionState.CONNECTING)
        try:
            ws = await self._connect(self._url)
        except Exception:
            await self._schedule_reconnect(gen)
            return
        if gen != self._generation or self._closed:
            await _safe_close(ws)
            return
        try:
            await self._send_raw(ws, _auth_frame(token, self._db))
            outcome = await self._await_auth(ws, gen)
            if outcome != "authOk":
                await _safe_close(ws)
                if outcome == "terminal":
                    self._set_state(ConnectionState.IDLE)
                    self._reject_all_pending("authentication failed")
                    self._closed = True  # terminal: stop the loop
                return
            self._ws = ws
            self._user = self._user  # set in _await_auth on authOk
            self._set_state(ConnectionState.CONNECTED)
            self._attempt = 0
            self._last_pong = self._now()
            await self._flush_on_auth()
            await self._run_session(ws, gen)
        except _PeerClosed as e:
            await self._on_peer_closed(e.code, gen)
        except Exception:
            await self._schedule_reconnect(gen)

    async def _await_auth(self, ws: Connection, gen: int) -> str:
        deadline = self._now() + _AUTH_DEADLINE
        while True:
            remaining = deadline - self._now()
            if remaining <= 0:
                return "reconnect"
            try:
                raw = await ws.recv()
            except _PeerClosed as e:
                return "terminal" if e.code == _AUTH_FAILED_CODE else "reconnect"
            msg = _SERVER.validate_json(raw)
            tag = _tag(msg)
            if tag == "authOk":
                self._user = msg.user
                return "authOk"
            if tag == "authErr":
                return "terminal"
            # During the handshake window tolerate non-auth frames (e.g. pong).
            self._dispatch(msg)

    async def _run_session(self, ws: Connection, gen: int) -> None:
        self._last_pong = self._now()
        reader = asyncio.create_task(self._read_loop(ws, gen))
        heartbeat = asyncio.create_task(self._heartbeat_loop(ws, gen))
        done, pending = await asyncio.wait({reader, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        code = _AUTH_FAILED_CODE  # default if heart/reader exited oddly
        for t in done:
            code = t.result() if not t.cancelled() else code
        await _safe_close(ws)
        self._ws = None
        await self._on_peer_closed(code, gen)

    async def _read_loop(self, ws: Connection, gen: int) -> int:
        try:
            while gen == self._generation and not self._closed:
                raw = await ws.recv()
                self._dispatch(_SERVER.validate_json(raw))
        except _PeerClosed as e:
            return e.code
        return 1000

    async def _heartbeat_loop(self, ws: Connection, gen: int) -> int:
        if self._heartbeat <= 0:
            return await asyncio.Future()  # sleeps forever (cancelled)
        while gen == self._generation and not self._closed:
            await self._sleep(self._heartbeat)
            if self._now() - self._last_pong >= self._heartbeat * 2:
                await _safe_close(ws, 4000)
                return 4000
            await self._send(_ping_frame())
        return 1000

    async def _schedule_reconnect(self, gen: int) -> None:
        if self._closed or gen != self._generation:
            return
        self._set_state(ConnectionState.RECONNECTING)
        delay = _backoff_delay(self._attempt, self._backoff_base, self._backoff_max, self._random())
        self._attempt += 1
        await self._sleep(delay)
        # the outer _drive loop re-enters _epoch on the next iteration

    async def _on_peer_closed(self, code: int, gen: int) -> None:
        self._ws = None
        self._reject_inflight("connection closed before acknowledgment")
        if code == _AUTH_FAILED_CODE:
            self._set_state(ConnectionState.IDLE)
            self._closed = True  # terminal
        else:
            await self._schedule_reconnect(gen)

    # --- sends ----------------------------------------------------------

    async def _send(self, frame: str) -> None:
        ws = self._ws
        if ws is None:
            return
        await self._send_raw(ws, frame)

    async def _send_raw(self, ws: Connection, frame: str) -> None:
        async with self._send_lock:
            try:
                await ws.send(frame)
            except _PeerClosed:
                pass  # the read loop drives reconnection

    async def _flush_on_auth(self) -> None:
        # Resubscribe every active query, then flush queued mutations/schedules.
        for sub in list(self._subs_by_id.values()):
            await self._send(sub.frame)
            sub.subscribed = True
        for mp in list(self._pending_mut.values()):
            if not mp.sent:
                await self._send(mp.frame)
                mp.sent = True
        for sp in list(self._pending_sched.values()):
            if not sp.sent:
                await self._send(sp.frame)
                sp.sent = True

    # --- dispatch (forward-compatible; Tasks 3-4 fill the maps) ---------

    def _dispatch(self, msg: Any) -> None:
        tag = _tag(msg)
        if tag == "pong":
            self._last_pong = self._now()
        elif tag == "queryUpdate":
            self._on_query_update(msg)
        elif tag == "subscribeErr":
            self._on_subscribe_err(msg)
        elif tag == "mutateOk":
            self._on_mutate_ok(msg)
        elif tag == "mutateErr":
            self._on_mutate_err(msg)
        elif tag in ("scheduleOk", "scheduleErr", "scheduleAck", "listSchedulesOk"):
            self._on_sched(msg)
        # authOk/authErr/pong-noop handled elsewhere; unknown tags ignored.

    # Populated in Task 3:
    def _on_query_update(self, msg: Any) -> None: ...
    def _on_subscribe_err(self, msg: Any) -> None: ...
    # Populated in Task 4:
    def _on_mutate_ok(self, msg: Any) -> None: ...
    def _on_mutate_err(self, msg: Any) -> None: ...
    def _on_sched(self, msg: Any) -> None: ...
    def _reject_all_pending(self, reason: str) -> None: ...
    def _reject_inflight(self, reason: str) -> None: ...

    def _set_state(self, state: ConnectionState) -> None:
        self._state = state


def _tag(msg: Any) -> str:
    return getattr(msg, "type", "")


async def _safe_close(ws: Connection, code: int = 1000) -> None:
    try:
        await ws.close(code, "")
    except Exception:
        pass


def _auth_frame(token: str, db: str) -> str:
    from .wire import _ClientAuth
    return _ClientAuth(token=token, db=db).model_dump_json(by_alias=True)


def _ping_frame() -> str:
    from .wire import _ClientPing
    return _ClientPing().model_dump_json(by_alias=True)


async def _default_connect(url: str) -> Connection:
    try:
        import websockets
        from websockets.exceptions import ConnectionClosed
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "websockets is required for RtDbClient: install with `pip install par-rt-db[ws]`"
        ) from e
    raw = await websockets.connect(url)
    return _RealConn(raw, ConnectionClosed)


class _RealConn:
    """Adapts a ``websockets`` connection to the ``Connection`` protocol,
    translating ``ConnectionClosed`` into ``_PeerClosed``."""

    def __init__(self, raw: Any, closed_exc: type[BaseException]) -> None:
        self._raw = raw
        self._closed = closed_exc

    async def send(self, data: str) -> None:
        try:
            await self._raw.send(data)
        except self._closed as e:
            raise _PeerClosed(_ws_close_code(e), "") from e

    async def recv(self) -> str:
        try:
            return await self._raw.recv()
        except self._closed as e:
            raise _PeerClosed(_ws_close_code(e), "") from e

    async def close(self, code: int = 1000, reason: str = "") -> None:
        try:
            await self._raw.close(code, reason)
        except self._closed:
            pass


def _ws_close_code(exc: BaseException) -> int:
    return int(getattr(exc, "code", 1000) or 1000)
```

Notes for the implementer:
- `_drive`'s `await self._sleep(0)` when there is no token just yields once and re-loops; a real "park until poked" is not needed for v1 (callers supply a token). Keep the loop correct: never busy-spin without yielding.
- `asyncio` must be imported at top: add `import asyncio`.
- The `_pending_mut`/`_pending_sched`/`_Sub` types referenced in `_flush_on_auth` are defined in Tasks 3–4; here they are empty dicts so `_flush_on_auth` is a no-op. Use `getattr(mp, "sent", True)` guards if needed to avoid AttributeError on the empty maps (the `for ... values()` loops simply don't execute).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py -q`
Expected: PASS.

- [ ] **Step 5: Lint + typecheck**

Run: `cd python-client && uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean. (If pyright flags the forward-referenced `_Sub`/`_MutPending` attributes, type those maps as `dict[str, Any]` — already the case.)

- [ ] **Step 6: Commit**

```bash
git add python-client/src/par_rt_db/ws_client.py python-client/tests/test_ws_routing.py
git commit -m "feat(python-client): reactive WS connection core (handshake, reconnect, heartbeat)"
```

---

## Task 3: Subscriptions — `subscribe()`, `Subscription`, dedup, refcount, subscribeErr

**Files:**
- Modify: `python-client/src/par_rt_db/ws_client.py`
- Test: `python-client/tests/test_ws_routing.py` (add subscription tests)

**Interfaces:**
- Consumes: `_dump_query`, `_terminal_of` (from `par_rt_db.query`); `parse_result` (from `par_rt_db.query`); `_ClientSubscribe`, `_ClientUnsubscribe` (from `wire`); `RtDbError`; the driver's `_send` and `_flush_on_auth` (which already iterate `self._subs_by_id`).
- Produces: `Subscription` (public: `current()`, `error()`, `__aiter__`, `__anext__`, `unsubscribe()`, async context-manager), `_Sub` dataclass, and `RtDbClient.subscribe(query, *, model=dict) -> Subscription`. Fills in `_on_query_update`, `_on_subscribe_err`, and the unsubscribe/decref path.

- [ ] **Step 1: Write the failing subscription tests**

Add to `test_ws_routing.py` (reuse `make_client`, `_drain`):
```python
from par_rt_db import TableQuery
from par_rt_db.ws_client import RtDbError


async def _connected(conn):
    client, _ = make_client(conn)
    await client.connect()
    await _drain()
    await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    return client


async def test_subscribe_sends_frame_and_delivers_first_value():
    conn = FakeConn()
    client = await _connected(conn)
    sub = client.subscribe(TableQuery("items").collect())
    await _drain()
    assert any('"type":"subscribe"' in f for f in conn.sent)
    assert sub.current() is None
    await conn.deliver('{"type":"queryUpdate","queryId":"' + _qid(conn, "subscribe") + '","result":[]}')
    await _drain()
    assert sub.current() == []


async def test_subscribe_err_raises_from_iterator_and_exposes_error():
    conn = FakeConn()
    client = await _connected(conn)
    sub = client.subscribe(TableQuery("items").collect())
    await _drain()
    qid = _qid(conn, "subscribe")
    await conn.deliver('{"type":"subscribeErr","queryId":"' + qid + '","error":{"code":"BAD_REQUEST","message":"bad index"}}')
    await _drain()
    assert isinstance(sub.error(), RtDbError)
    with pytest.raises(RtDbError):
        async for _ in sub:
            pass


async def test_identical_queries_share_one_subscription():
    conn = FakeConn()
    client = await _connected(conn)
    s1 = client.subscribe(TableQuery("items").collect())
    s2 = client.subscribe(TableQuery("items").collect())
    await _drain()
    subscribe_frames = [f for f in conn.sent if '"type":"subscribe"' in f]
    assert len(subscribe_frames) == 1
    s1.unsubscribe()
    await _drain()
    # Still one active listener -> no unsubscribe frame yet.
    assert not any('"type":"unsubscribe"' in f for f in conn.sent)
    s2.unsubscribe()
    await _drain()
    assert any('"type":"unsubscribe"' in f for f in conn.sent)


async def test_reconnect_resubscribes_active_queries():
    conn = FakeConn()
    client = await _connected(conn)
    client.subscribe(TableQuery("items").collect())
    await _drain()
    first = [f for f in conn.sent if '"type":"subscribe"' in f]
    # Force a reconnectable drop.
    conn2 = FakeConn()
    client._connect = lambda url: _ready(conn2)  # next epoch uses the new socket
    conn.close_code = 4000
    await conn._inbox.put(None)
    await _drain()
    # Drive the reconnect backoff instantly (FakeClock.sleep wakes on advance).
    await conn2.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    assert any('"type":"subscribe"' in f for f in conn2.sent)
    await client.close()
```
Helper to extract the queryId the client assigned:
```python
def _qid(conn, typ):
    for f in conn.sent:
        d = json.loads(f)
        if d.get("type") == typ:
            return d["queryId"]
    raise AssertionError("no frame of type " + typ)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py -q`
Expected: FAIL — `subscribe` not defined / `_on_query_update` is a no-op so values never land.

- [ ] **Step 3: Implement subscriptions**

Add to `ws_client.py`:
```python
from dataclasses import dataclass, field

from .query import _dump_query, _terminal_of, parse_result
from .wire import Query as _Query  # noqa: F401  (for isinstance checks vs TableQuery)
```
(Adjust the `Query` import to the real one; `TableQuery` is also needed for the `isinstance` check — import both from `.query`.)

Define the internal sub state and the public `Subscription`:
```python
@dataclass
class _Sub:
    query_id: str
    key: str
    frame: str
    terminal: str
    model: type
    refcount: int = 0
    value: Any = None
    error: RtDbError | None = None
    version: int = 0
    subscribed: bool = False
    closed: bool = False
    cond: asyncio.Condition = field(default_factory=lambda: asyncio.Condition())


class Subscription:
    """A live query: async-iterator + latest value."""

    def __init__(self, client: "RtDbClient", sub: _Sub) -> None:
        self._client = client
        self._sub = sub
        self._iter_version = 0

    def current(self) -> Any | None:
        return self._sub.value

    def error(self) -> RtDbError | None:
        return self._sub.error

    def unsubscribe(self) -> None:
        self._client._decref(self._sub.key)

    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.unsubscribe()

    def __aiter__(self) -> "Subscription":
        self._iter_version = 0  # yield the current value (if any) first
        return self

    async def __anext__(self) -> Any:
        sub = self._sub
        async with sub.cond:
            while sub.version <= self._iter_version and sub.error is None and not sub.closed:
                await sub.cond.wait()
        if sub.closed:
            raise StopAsyncIteration
        if sub.error is not None:
            raise sub.error
        self._iter_version = sub.version
        return sub.value
```

Add `subscribe` + the handlers + decref to `RtDbClient`:
```python
    def subscribe(self, query: Any, *, model: type = dict) -> Subscription:
        from .query import Query, TableQuery
        qd = _dump_query(query)
        key = _canonical_key(qd)
        built = query.build() if isinstance(query, TableQuery) else query
        terminal = _terminal_of(built)
        sub = self._subs_by_key.get(key)
        if sub is None:
            self._counter += 1
            qid = f"sub-{self._counter}"
            from .wire import _ClientSubscribe
            frame = _ClientSubscribe(query_id=qid, query=qd).model_dump_json(by_alias=True)
            sub = _Sub(query_id=qid, key=key, frame=frame, terminal=terminal, model=model)
            self._subs_by_key[key] = sub
            self._subs_by_id[qid] = sub
            if self._state is ConnectionState.CONNECTED:
                sub.subscribed = True
                asyncio.get_running_loop().create_task(self._send(frame))
        sub.refcount += 1
        return Subscription(self, sub)

    def _decref(self, key: str) -> None:
        sub = self._subs_by_key.get(key)
        if sub is None:
            return
        sub.refcount -= 1
        if sub.refcount <= 0:
            self._subs_by_key.pop(key, None)
            self._subs_by_id.pop(sub.query_id, None)
            sub.closed = True
            self._notify(sub)
            if sub.subscribed and self._state is ConnectionState.CONNECTED:
                from .wire import _ClientUnsubscribe
                frame = _ClientUnsubscribe(query_id=sub.query_id).model_dump_json(by_alias=True)
                asyncio.get_running_loop().create_task(self._send(frame))

    def _on_query_update(self, msg: Any) -> None:
        sub = self._subs_by_id.get(msg.query_id)
        if sub is None:
            return
        sub.value = parse_result(sub.model, sub.terminal, msg.result)
        sub.version += 1
        self._notify(sub)

    def _on_subscribe_err(self, msg: Any) -> None:
        sub = self._subs_by_id.get(msg.query_id)
        if sub is None:
            return
        sub.error = RtDbError.from_envelope(msg.error.model_dump())
        self._subs_by_key.pop(sub.key, None)
        self._subs_by_id.pop(sub.query_id, None)
        self._notify(sub)

    @staticmethod
    def _notify(sub: _Sub) -> None:
        async def _wake() -> None:
            async with sub.cond:
                sub.cond.notify_all()
        try:
            asyncio.get_running_loop().create_task(_wake())
        except RuntimeError:
            pass
```
Replace the Task-2 no-op stubs for `_on_query_update`/`_on_subscribe_err` with these.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py -q`
Expected: PASS.

- [ ] **Step 5: Lint + typecheck**

Run: `cd python-client && uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add python-client/src/par_rt_db/ws_client.py python-client/tests/test_ws_routing.py
git commit -m "feat(python-client): reactive WS subscriptions (async iterator, dedup, subscribeErr->RtDbError)"
```

---

## Task 4: Mutations + schedule ops (at-most-once)

**Files:**
- Modify: `python-client/src/par_rt_db/ws_client.py`
- Test: `python-client/tests/test_ws_routing.py` (add mutate + schedule tests)

**Interfaces:**
- Consumes: `Transaction` (from `par_rt_db.mutation`), `StepResult` + a `TypeAdapter(StepResult)` (mirror `http_client._STEP_RESULT_ADAPTER`), `ScheduleWhen` + `ScheduleInfo` (from `wire`), the `_Client*` schedule models, `RtDbError`, the driver's `_send`/`_flush_on_auth`/`_reject_inflight`/`_reject_all_pending`.
- Produces: `RtDbClient.mutate`, `schedule`, `cancel_schedule`, `pause_schedule`, `resume_schedule`, `list_schedules`, and the `_MutPending`/`_SchedPending` dataclasses; fills `_on_mutate_ok`, `_on_mutate_err`, `_on_sched`, `_reject_inflight`, `_reject_all_pending`.

**At-most-once contract (must hold):**
- A mutate/schedule call registers a future keyed by its id and, when connected, sends the frame immediately and marks it `sent=True`.
- While disconnected, the future stays `sent=False` (queued) and the frame is sent on the next `_flush_on_auth`.
- On a reconnectable socket drop, only `sent=True` (in-flight) entries are rejected with `RtDbError(INTERNAL, "connection closed before acknowledgment")`; queued entries survive.
- On `close()`, all pending (queued + in-flight) are rejected with `"client closed"`.
- The optional `idempotency_key` is the wire `idempotencyKey` (for safe caller retry); `mutId`/`scheduleId` are per-client correlation ids (`mut-{n}`, `sch-{n}`).

- [ ] **Step 1: Write the failing tests**

Add to `test_ws_routing.py`:
```python
from par_rt_db import Mutation, Transaction


def _insert_txn():
    return Mutation().insert("items", {"_id": "i1", "n": 1}).build()


async def test_mutate_resolves_on_mutate_ok():
    conn = FakeConn()
    client = await _connected(conn)
    task = asyncio.create_task(client.mutate(_insert_txn()))
    await _drain()
    mid = _id(conn, "mutate")
    await conn.deliver('{"type":"mutateOk","mutId":"' + mid + '","results":[{"op":"insert","id":"i1"}]}')
    await _drain()
    results = await asyncio.wait_for(task, 1.0)
    assert results[0].id == "i1"


async def test_mutate_rejects_on_mutate_err():
    conn = FakeConn()
    client = await _connected(conn)
    task = asyncio.create_task(client.mutate(_insert_txn()))
    await _drain()
    mid = _id(conn, "mutate")
    await conn.deliver('{"type":"mutateErr","mutId":"' + mid + '","error":{"code":"NOT_FOUND","message":"no table"}}')
    with pytest.raises(RtDbError):
        await asyncio.wait_for(task, 1.0)


async def test_inflight_mutate_rejected_on_drop_queued_survives():
    conn = FakeConn()
    client, clock = make_client(conn)
    await client.connect()
    await _drain()
    await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    # One in-flight mutate (sent), then drop.
    inflight = asyncio.create_task(client.mutate(_insert_txn()))
    await _drain()
    conn.close_code = 4000
    await conn._inbox.put(None)
    await _drain()
    with pytest.raises(RtDbError):
        await asyncio.wait_for(inflight, 1.0)


async def test_schedule_lifecycle():
    conn = FakeConn()
    client = await _connected(conn)
    sch = asyncio.create_task(client.schedule(_insert_txn(), ("afterMs", {"ms": 100})))
    await _drain()
    sid = _id(conn, "schedule")
    await conn.deliver('{"type":"scheduleOk","scheduleId":"' + sid + '","id":"job-1"}')
    job_id = await asyncio.wait_for(sch, 1.0)
    assert job_id == "job-1"
    cancel = asyncio.create_task(client.cancel_schedule("job-1"))
    await _drain()
    ack_sid = _id(conn, "cancelSchedule")
    await conn.deliver('{"type":"scheduleAck","scheduleId":"' + ack_sid + '","ok":true}')
    await asyncio.wait_for(cancel, 1.0)  # resolves without error
```
Helpers:
```python
def _id(conn, typ):
    key = {"mutate": "mutId", "schedule": "scheduleId", "cancelSchedule": "scheduleId"}[typ]
    for f in conn.sent:
        d = json.loads(f)
        if d.get("type") == typ:
            return d[key]
    raise AssertionError("no frame of type " + typ)
```
(Note: `ScheduleWhen` is a tagged union; in the test we pass a tuple `(tag, fields)` only if the public `schedule()` accepts that shape — otherwise build it via the wire helper. See Step 3 for the accepted argument shape.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py -q`
Expected: FAIL — `mutate` not defined.

- [ ] **Step 3: Implement mutate + schedule ops**

Add to `ws_client.py`:
```python
from .mutation import StepResult, Transaction
from .wire import ScheduleInfo, ScheduleWhen, _ClientCancelSchedule, _ClientListSchedules, _ClientMutate, _ClientPauseSchedule, _ClientResumeSchedule, _ClientSchedule

_STEP_RESULT_ADAPTER = TypeAdapter(StepResult)


@dataclass
class _MutPending:
    future: asyncio.Future
    frame: str
    sent: bool = False


@dataclass
class _SchedPending:
    future: asyncio.Future
    frame: str
    sent: bool = False
    kind: str = ""  # "schedule" | "cancel" | "pause" | "resume" | "list"
```

Add to `RtDbClient`:
```python
    async def mutate(self, txn: Transaction, *, idempotency_key: str | None = None) -> list[StepResult]:
        self._counter += 1
        mid = f"mut-{self._counter}"
        frame = _ClientMutate(mut_id=mid, idempotency_key=idempotency_key, txn=txn.model_dump(by_alias=True, mode="json")).model_dump_json(by_alias=True)
        fut = asyncio.get_running_loop().create_future()
        self._pending_mut[mid] = _MutPending(fut, frame)
        await self._dispatch_send(mid, self._pending_mut)
        return await fut

    async def _dispatch_send(self, mid: str, table: dict[str, Any]) -> None:
        mp = table[mid]
        if self._state is ConnectionState.CONNECTED:
            await self._send(mp.frame)
            mp.sent = True
        # else: queued; flushed on next authOk

    async def schedule(self, txn: Transaction, when: ScheduleWhen) -> str:
        return await self._sched_op("schedule", txn=txn, when=when)  # type: ignore[arg-type]

    async def cancel_schedule(self, id: str) -> None:
        await self._sched_op("cancel", id=id)

    async def pause_schedule(self, id: str) -> None:
        await self._sched_op("pause", id=id)

    async def resume_schedule(self, id: str) -> None:
        await self._sched_op("resume", id=id)

    async def list_schedules(self) -> list[ScheduleInfo]:
        return await self._sched_op("list")  # type: ignore[return-value]

    async def _sched_op(self, kind: str, **fields: Any) -> Any:
        self._counter += 1
        sid = f"sch-{self._counter}"
        frame = _build_sched_frame(kind, sid, fields)
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending_sched[sid] = _SchedPending(fut, frame, kind=kind)
        await self._dispatch_send(sid, self._pending_sched)
        return await fut
```
Frame builder + result coercion:
```python
def _build_sched_frame(kind: str, sid: str, fields: dict[str, Any]) -> str:
    if kind == "schedule":
        return _ClientSchedule(schedule_id=sid, when=fields["when"], txn=fields["txn"].model_dump(by_alias=True, mode="json")).model_dump_json(by_alias=True)
    if kind == "cancel":
        return _ClientCancelSchedule(schedule_id=sid, id=fields["id"]).model_dump_json(by_alias=True)
    if kind == "pause":
        return _ClientPauseSchedule(schedule_id=sid, id=fields["id"]).model_dump_json(by_alias=True)
    if kind == "resume":
        return _ClientResumeSchedule(schedule_id=sid, id=fields["id"]).model_dump_json(by_alias=True)
    return _ClientListSchedules(schedule_id=sid).model_dump_json(by_alias=True)
```
Fill the handlers (replace Task-2 stubs):
```python
    def _on_mutate_ok(self, msg: Any) -> None:
        mp = self._pending_mut.pop(msg.mut_id, None)
        if mp is not None and not mp.future.done():
            mp.future.set_result([_STEP_RESULT_ADAPTER.validate_python(r) for r in msg.results])

    def _on_mutate_err(self, msg: Any) -> None:
        mp = self._pending_mut.pop(msg.mut_id, None)
        if mp is not None and not mp.future.done():
            mp.future.set_exception(RtDbError.from_envelope(msg.error.model_dump()))

    def _on_sched(self, msg: Any) -> None:
        tag = _tag(msg)
        sp = self._pending_sched.pop(msg.schedule_id, None)
        if sp is None or sp.future.done():
            return
        if tag == "scheduleOk":
            sp.future.set_result(msg.id)
        elif tag == "listSchedulesOk":
            sp.future.set_result(list(msg.schedules))
        elif tag == "scheduleErr":
            sp.future.set_exception(RtDbError.from_envelope(msg.error.model_dump()))
        elif tag == "scheduleAck":
            if msg.ok:
                sp.future.set_result(None)
            else:
                err = msg.error.model_dump() if msg.error is not None else {"code": "INTERNAL", "message": "schedule ack failed"}
                sp.future.set_exception(RtDbError.from_envelope(err))

    def _reject_inflight(self, reason: str) -> None:
        for mp in list(self._pending_mut.values()):
            if mp.sent and not mp.future.done():
                self._pending_mut.pop(mp.future.__class__ and _mid_of(mp), None)
                mp.future.set_exception(RtDbError(ErrorCode.INTERNAL, reason))
        for sp in list(self._pending_sched.values()):
            if sp.sent and not sp.future.done():
                sp.future.set_exception(RtDbError(ErrorCode.INTERNAL, reason))

    def _reject_all_pending(self, reason: str) -> None:
        for mp in list(self._pending_mut.values()):
            if not mp.future.done():
                mp.future.set_exception(RtDbError(ErrorCode.INTERNAL, reason))
        self._pending_mut.clear()
        for sp in list(self._pending_sched.values()):
            if not sp.future.done():
                sp.future.set_exception(RtDbError(ErrorCode.INTERNAL, reason))
        self._pending_sched.clear()
```
(`_reject_inflight` must pop in-flight mut entries by their key — track the id on `_MutPending` instead of the awkward `_mid_of` lookup. Add `id: str` to `_MutPending` and pop by `mp.id`. Simplify the loop to `self._pending_mut.pop(mp.id, None)`.) Apply the same `id` field to `_SchedPending` if convenient (the schedule_id is already the key).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py -q`
Expected: PASS.

- [ ] **Step 5: Lint + typecheck**

Run: `cd python-client && uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add python-client/src/par_rt_db/ws_client.py python-client/tests/test_ws_routing.py
git commit -m "feat(python-client): reactive WS at-most-once mutations + schedule ops"
```

---

## Task 5: Public export, live integration test, docs, parity matrix

**Files:**
- Modify: `python-client/src/par_rt_db/__init__.py`
- Modify: `python-client/pyproject.toml` (register `live` marker)
- Create: `python-client/tests/test_ws_integration.py`
- Modify: `FEATURE_MATRIX.md`
- Modify: `python-client/README.md`

**Interfaces:**
- Consumes: the finished `RtDbClient` + `Subscription` from `ws_client`.

- [ ] **Step 1: Register the live marker**

In `python-client/pyproject.toml`, under `[tool.pytest.ini_options]`, add:
```toml
markers = [
    "live: opt-in live-server integration tests (set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY)",
]
```

- [ ] **Step 2: Add the lazy export**

In `python-client/src/par_rt_db/__init__.py`, add `RtDbClient` and `Subscription` to `__all__`, and extend `__getattr__`:
```python
def __getattr__(name: str) -> Any:
    if name == "RtDbHttpClient":
        from . import http_client
        return http_client.RtDbHttpClient
    if name in ("RtDbClient", "Subscription"):
        from . import ws_client
        return getattr(ws_client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```
Also add the `TYPE_CHECKING` import: `from .ws_client import RtDbClient as _RtDbClient, Subscription as _Subscription` under the existing `if TYPE_CHECKING:` block (keep websockets optional).

- [ ] **Step 3: Write the live integration test**

Create `python-client/tests/test_ws_integration.py`:
```python
"""Opt-in live-server test for the reactive WS client.

Gated on RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY. Run:
    make dev-db-up   # Postgres on 55434
    # start the server on :8300 with RTDB_ADMIN_KEY=dev-admin-key
    RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 \\
    RTDB_TEST_ADMIN_KEY=dev-admin-key \\
    uv run pytest tests/test_ws_integration.py -q -m live
"""

import asyncio
import os

import pytest

pytestmark = pytest.mark.skipif(
    not (os.environ.get("RTDB_TEST_SERVER_URL") and os.environ.get("RTDB_TEST_ADMIN_KEY")),
    reason="set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY to run live WS tests",
)


@pytest.mark.live
async def test_subscribe_and_live_update():
    from par_rt_db import Mutation, TableQuery
    from par_rt_db.http_client import RtDbHttpClient
    from par_rt_db.ws_client import RtDbClient

    url = os.environ["RTDB_TEST_SERVER_URL"]
    admin_key = os.environ["RTDB_TEST_ADMIN_KEY"]
    import uuid
    db = "t" + uuid.uuid4().hex[:12]

    admin = RtDbHttpClient(url, db, admin_key)
    admin.create_db(db)
    admin.push_schema(db, {"tables": [{"name": "items", "fields": [{"name": "n", "type": "number"}]}]})
    token = admin.mint_token(db)

    try:
        async def get_token():
            return token

        client = RtDbClient(url, db, get_token=get_token)
        await client.connect()
        sub = client.subscribe(TableQuery("items").collect())
        try:
            # Wait for the initial (empty) push.
            await asyncio.wait_for(_first(sub), 10.0)
            assert sub.current() == []
            # Insert over WS and await the live update.
            await client.mutate(Mutation().insert("items", {"_id": "i1", "n": 1}).build())
            await asyncio.wait_for(_next_nonempty(sub), 10.0)
            assert any(d.get("_id") == "i1" for d in sub.current())
        finally:
            await client.close()
    finally:
        admin.delete_db(db)


async def _first(sub):
    async for v in sub:
        return v


async def _next_nonempty(sub):
    async for v in sub:
        if v:
            return v
```

- [ ] **Step 4: Run the no-socket gate**

Run: `cd python-client && uv run pytest tests/test_ws_routing.py tests/test_ws_integration.py -q`
Expected: PASS for `test_ws_routing.py`; the live test is **skipped** (no env). Then the full python gate:
Run: `make python-client-checkall`
Expected: clean (fmt-check, ruff, pyright, pytest all green).

- [ ] **Step 5: Run the live test against a real dev server**

```bash
make dev-db-up
# In another shell, start the server (admin key + port 8300) — see deploy/README.md.
cd python-client && RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 RTDB_TEST_ADMIN_KEY=dev-admin-key uv run pytest tests/test_ws_integration.py -q -m live
```
Expected: PASS (one live test exercises subscribe → initial push → WS mutate → live update). If starting the server locally is not feasible in the session, leave this step to manual verification and note it.

- [ ] **Step 6: Update docs**

- `FEATURE_MATRIX.md`: flip the Python-client rows for reactive WebSocket / live queries / mutations-over-WS / schedule ops ❌→✅ with "Mirrored across: ✅ts ✅rust ✅python".
- `python-client/README.md`: add a `[ws]` extra usage snippet:
```python
import asyncio
from par_rt_db import Mutation, TableQuery
from par_rt_db.ws_client import RtDbClient

async def main():
    client = RtDbClient("wss://rtdb.example.com", "mydb", get_token=lambda: _token())
    await client.connect()
    async with client.subscribe(TableQuery("items").collect()) as sub:
        await client.mutate(Mutation().insert("items", {"_id": "i1"}).build())
        async for value in sub:
            print(value)
    await client.close()
```

- [ ] **Step 7: Commit**

```bash
git add python-client/src/par_rt_db/__init__.py python-client/pyproject.toml python-client/tests/test_ws_integration.py FEATURE_MATRIX.md python-client/README.md
git commit -m "feat(python-client): export reactive WS client; live integration test; docs/parity"
```

---

## Self-Review (completed)

- **Spec coverage:** spec §Reactive WS (lines 241–273) — `RtDbClient(url,db,get_token,*,heartbeat,backoff_base,backoff_max)` (Task 2); async `get_token` (Task 2); URL flip + `/sync`, plain WS no subprotocol (Task 2 `_sync_url`/`_default_connect`); auth-first-frame + authOk/authErr + 4401 terminal (Task 2 `_await_auth`/`_on_peer_closed`); jittered backoff formula + attempt reset (Task 1 helper + Task 2 `_schedule_reconnect`); resubscribe on reconnect (Task 2 `_flush_on_auth`); ping/`2×heartbeat`→4000 reconnect (Task 2 `_heartbeat_loop`); generation guard (Task 2 `_generation`); `subscribe`→`Subscription` async-iter + `.current()` + dedup + `sub-{n}` + unsubscribe-on-last (Task 3); first-queryUpdate-delivers-first-value (Task 3); subscribeErr→RtDbError + removal (Task 3, the recorded decision); at-most-once mutate `mut-{n}` + idempotencyKey + reject-inflight/queue (Task 4); schedule ops mirror mutate (Task 4). Out-of-scope items (optimistic, admin `/admin/stream`, sync wrappers, presence) confirmed not in any task. ✓
- **Placeholder scan:** none — every step has real code or an exact command. ✓
- **Type consistency:** `_Sub`/`Subscription`/`_MutPending`/`_SchedPending`/`_dispatch_send`/`_flush_on_auth`/`_reject_inflight`/`_reject_all_pending` names match across Tasks 2–4. The `_MutPending.id` addition called out in Task 4 Step 3 is the one refinement to apply when implementing. ✓
