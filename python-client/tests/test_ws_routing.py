"""No-socket unit tests for the reactive WS client (framing, backoff, dedup, routing)."""

import asyncio
import json

from par_rt_db.ws_client import (
    ConnectionState,
    RtDbClient,
    _backoff_delay,
    _canonical_key,
    _PeerClosed,
    _sync_url,
)


def test_sync_url_flips_scheme_and_appends_sync():
    assert _sync_url("http://localhost:8300") == "ws://localhost:8300/sync"
    assert _sync_url("https://rtdb.pardev.net") == "wss://rtdb.pardev.net/sync"
    assert _sync_url("ws://localhost:8300/") == "ws://localhost:8300/sync"
    assert _sync_url("wss://rtdb.pardev.net///") == "wss://rtdb.pardev.net/sync"


def test_canonical_key_is_order_independent():
    a = {"table": "items", "index": "by_x", "eq": [1]}
    b = {"index": "by_x", "eq": [1], "table": "items"}
    assert _canonical_key(a) == _canonical_key(b)


def test_canonical_key_distinguishes_different_shapes():
    assert _canonical_key({"table": "a"}) != _canonical_key({"table": "b"})


def test_backoff_delay_is_bounded_and_jittered():
    base, top = 0.5, 15.0
    # rand = 0.5 -> multiplier (0.5 + 0.5*0.5) = 0.75 of the raw cap.
    assert _backoff_delay(0, base, top, 0.5) == 0.375
    # attempt grows exponentially until capped at `top`; jitter in [0.5, 1.0] of raw.
    raw = min(top, base * (2**5))
    lo, hi = raw * 0.5, raw * 1.0
    assert lo <= _backoff_delay(5, base, top, 0.0) <= hi
    # never exceeds `top`.
    assert _backoff_delay(50, base, top, 1.0) <= top


# --- Connection-core tests (driver, handshake, reconnect, heartbeat) ----------
#
# Timing is driven with REAL asyncio timers + small config values + wait_for,
# not a fake clock. connect is injectable (FakeConn); random is injectable
# (deterministic jitter). sleep/now default to the stdlib implementations.


class FakeConn:
    """Stand-in Connection: records sent frames, lets tests feed inbound frames.

    Putting ``None`` on the inbox makes the next ``recv`` raise ``_PeerClosed``
    with this connection's ``close_code`` (how tests simulate a peer drop).
    """

    def __init__(self) -> None:
        self.sent: list[str] = []
        self._inbox: asyncio.Queue[str | None] = asyncio.Queue()
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


async def _const_token() -> str:
    return "tok"


def make_client(conn: FakeConn, *, rand: float = 0.5, **kw: object) -> tuple[RtDbClient, list[str]]:
    """Wire a client to ``conn`` with small timers; returns (client, connect_calls)."""
    calls: list[str] = []

    async def _connect(url: str) -> FakeConn:
        calls.append(url)
        return conn

    defaults: dict[str, object] = {"heartbeat": 10.0, "backoff_base": 0.01, "backoff_max": 0.05}
    defaults.update(kw)
    client = RtDbClient(
        "http://x",
        "db",
        get_token=_const_token,
        connect=_connect,
        random=lambda: rand,
        **defaults,  # type: ignore[arg-type]
    )
    return client, calls


async def _drain(n: int = 16) -> None:
    """Yield to the event loop so the driver task makes progress."""
    for _ in range(n):
        await asyncio.sleep(0)


async def _wait_until(pred: object, timeout: float = 2.0) -> None:
    """Poll ``pred`` every ~5ms until truthy or ``timeout`` (raises on timeout)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():  # type: ignore[operator]
        if loop.time() >= deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(0.005)


async def test_connect_sends_auth_then_marks_connected():
    conn = FakeConn()
    client, _ = make_client(conn)
    try:
        await client.connect()
        await _drain()
        # The very first frame is the auth handshake.
        assert json.loads(conn.sent[0]) == {"type": "auth", "token": "tok", "db": "db"}
        assert client.status().state is ConnectionState.CONNECTING
        await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _drain()
        assert client.status().state is ConnectionState.CONNECTED
        status = client.status()
        assert status.user is not None
        assert status.user.kind == "machine"
    finally:
        await client.close()


async def test_autherr_is_terminal_no_reconnect():
    conn = FakeConn()
    client, calls = make_client(conn)
    try:
        await client.connect()
        await _drain()
        await conn.deliver('{"type":"authErr","error":{"code":"UNAUTHORIZED","message":"no"}}')
        await _drain()
        assert client.status().state is ConnectionState.IDLE
        # authErr is terminal: exactly one socket was opened, no backoff reconnect.
        assert len(calls) == 1
    finally:
        await client.close()
    # Still no reconnect after close().
    assert len(calls) == 1


async def test_reconnectable_close_schedules_backoff():
    conn1 = FakeConn()
    conn2 = FakeConn()
    client, calls = make_client(conn1, rand=0.5)
    try:
        await client.connect()
        await _drain()
        await conn1.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _drain()
        assert client.status().state is ConnectionState.CONNECTED

        # Swap the connect factory so the reconnect opens a fresh socket.
        async def _connect2(url: str) -> FakeConn:
            calls.append(url)
            return conn2

        client._connect = _connect2

        # Peer drops with a reconnectable (non-4401) close code.
        conn1.close_code = 4000
        await conn1._inbox.put(None)
        await _wait_until(lambda: client.status().state is ConnectionState.RECONNECTING)
        # A new auth frame eventually appears on the second socket (reconnect worked).
        await _wait_until(lambda: any('"type":"auth"' in f for f in conn2.sent))
        assert len(calls) == 2
    finally:
        await client.close()


async def test_pong_resets_liveness():
    conn = FakeConn()
    client, _ = make_client(conn, heartbeat=0.05)
    try:
        await client.connect()
        await _drain()
        await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _drain()
        assert client.status().state is ConnectionState.CONNECTED

        # A pong resets the liveness deadline; the first heartbeat ping fires.
        await conn.deliver('{"type":"pong"}')
        await _wait_until(lambda: any('"type":"ping"' in f for f in conn.sent))
        assert client.status().state is ConnectionState.CONNECTED

        # Refresh liveness and confirm a SECOND ping fires — without the pong
        # the heartbeat loop would have closed the socket at 2*heartbeat.
        await conn.deliver('{"type":"pong"}')
        await _wait_until(lambda: sum('"type":"ping"' in f for f in conn.sent) >= 2)
        assert client.status().state is ConnectionState.CONNECTED
    finally:
        await client.close()
