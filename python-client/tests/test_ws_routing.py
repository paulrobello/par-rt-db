"""No-socket unit tests for the reactive WS client (framing, backoff, dedup, routing)."""

import asyncio
import json

import pytest

from par_rt_db import AfterMs, Mutation, TableQuery, Transaction
from par_rt_db.ws_client import (
    ConnectionState,
    RtDbClient,
    RtDbError,
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


async def test_unauthenticated_drive_backs_off_not_busy_spins():
    """While get_token returns None, _drive applies backoff, not sleep(0).

    Regression guard for ARC-128: the unauthenticated _drive path previously
    called asyncio.sleep(0) in a tight loop while the token was None, instead
    of using the same jittered exponential backoff the reconnect path uses.
    """
    sleeps: list[float] = []

    async def _recording_sleep(delay: float) -> None:
        sleeps.append(delay)
        await asyncio.sleep(0)

    async def _always_none() -> str | None:
        return None

    conn = FakeConn()
    calls: list[str] = []

    async def _connect(url: str) -> FakeConn:
        calls.append(url)
        return conn

    client = RtDbClient(
        "http://x",
        "db",
        get_token=_always_none,
        heartbeat=10.0,
        backoff_base=0.01,
        backoff_max=0.05,
        connect=_connect,
        random=lambda: 0.5,
        sleep=_recording_sleep,
    )
    await client.connect()
    await _drain(20)
    await client.close()

    # The driver never opened a socket (no token), but it did sleep with
    # backoff on each iteration — never sleep(0).
    assert len(sleeps) >= 2, f"expected >=2 backoff sleeps, got {sleeps}"
    assert all(d > 0 for d in sleeps), f"expected backoff > 0, got {sleeps}"
    assert calls == [], "no socket should be opened while token is None"


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


# --- Subscription tests (Task 3) ----------------------------------------
#
# Drive a connected client with ``await conn.deliver(frame)`` then ``_drain()``
# so the read loop progresses, then assert on ``sub.current()``/``sub.error()``
# or on frames recorded in ``conn.sent``.


async def _connected(conn: FakeConn) -> RtDbClient:
    """Make a client, connect, and complete the auth handshake."""
    client, _ = make_client(conn)
    await client.connect()
    await _drain()
    await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    assert client.status().state is ConnectionState.CONNECTED
    return client


def _qid(conn: FakeConn, typ: str) -> str:
    """Extract the queryId the client assigned from a recorded frame."""
    for f in conn.sent:
        d = json.loads(f)
        if d.get("type") == typ:
            return d["queryId"]
    raise AssertionError(f"no frame of type {typ}")


async def test_subscribe_sends_frame_and_delivers_first_value():
    conn = FakeConn()
    client = await _connected(conn)
    try:
        sub = client.subscribe(TableQuery("items").collect())
        await _drain()
        # (1) subscribe() while connected sends exactly one subscribe frame.
        assert any('"type":"subscribe"' in f for f in conn.sent)
        # current() is None until the first queryUpdate lands.
        assert sub.current() is None

        qid = _qid(conn, "subscribe")
        await conn.deliver('{"type":"queryUpdate","queryId":"' + qid + '","result":[]}')
        await _drain()
        # An empty collect result parses to [].
        assert sub.current() == []

        # The async iterator yields that first value.
        got = None
        async for value in sub:
            got = value
            break
        assert got == []
    finally:
        await client.close()


async def test_subscribe_err_raises_from_iterator_and_exposes_error():
    conn = FakeConn()
    client = await _connected(conn)
    try:
        sub = client.subscribe(TableQuery("items").collect())
        await _drain()
        qid = _qid(conn, "subscribe")
        await conn.deliver(
            '{"type":"subscribeErr","queryId":"'
            + qid
            + '","error":{"code":"BAD_REQUEST","message":"bad index"}}'
        )
        await _drain()

        # sub.error() is an RtDbError carrying the envelope.
        err = sub.error()
        assert isinstance(err, RtDbError)
        assert err.message == "bad index"

        # async-for over the sub raises RtDbError.
        with pytest.raises(RtDbError):
            async for _ in sub:
                pass

        # The shape is removed: a subsequent reconnect does NOT resend it.
        conn2 = FakeConn()

        async def _connect2(url: str) -> FakeConn:
            return conn2

        client._connect = _connect2
        conn.close_code = 4000
        await conn._inbox.put(None)
        await _wait_until(lambda: any('"type":"auth"' in f for f in conn2.sent))
        await conn2.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: client.status().state is ConnectionState.CONNECTED)
        await _drain(20)
        assert not any('"type":"subscribe"' in f for f in conn2.sent)
    finally:
        await client.close()


async def test_identical_queries_share_one_subscription():
    conn = FakeConn()
    client = await _connected(conn)
    try:
        s1 = client.subscribe(TableQuery("items").collect())
        s2 = client.subscribe(TableQuery("items").collect())
        await _drain()
        # (3) Two identical shapes dedup to exactly one subscribe frame.
        subscribe_frames = [f for f in conn.sent if '"type":"subscribe"' in f]
        assert len(subscribe_frames) == 1

        s1.unsubscribe()
        await _drain()
        # First unsubscribe still leaves one listener -> no frame yet.
        assert not any('"type":"unsubscribe"' in f for f in conn.sent)

        s2.unsubscribe()
        await _drain()
        # The last unsubscribe sends the unsubscribe frame.
        assert any('"type":"unsubscribe"' in f for f in conn.sent)
    finally:
        await client.close()


async def test_reconnect_resubscribes_active_queries():
    conn = FakeConn()
    client = await _connected(conn)
    try:
        client.subscribe(TableQuery("items").collect())
        await _drain()
        assert any('"type":"subscribe"' in f for f in conn.sent)

        # (4) Force a reconnectable drop; the active query is re-subscribed on
        # the new socket after the handshake completes.
        conn2 = FakeConn()

        async def _connect2(url: str) -> FakeConn:
            return conn2

        client._connect = _connect2
        conn.close_code = 4000
        await conn._inbox.put(None)
        await _wait_until(lambda: any('"type":"auth"' in f for f in conn2.sent))
        await conn2.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: any('"type":"subscribe"' in f for f in conn2.sent))
        assert client.status().state is ConnectionState.CONNECTED
    finally:
        await client.close()


# --- Mutate + schedule ops (Task 4) -------------------------------------
#
# At-most-once contract: a mutate/schedule frame is sent once when connected
# (marked in-flight) and resolved by the matching server ack/ok/err. On a
# reconnectable drop only in-flight entries are rejected; queued entries
# (created while disconnected) survive and flush on the next authOk.


def _insert_txn() -> Transaction:
    """A trivial one-step insert transaction used across the mutate tests."""
    return Mutation.builder().insert("items", {"_id": "i1", "n": 1}).build()


def _id(conn: FakeConn, typ: str) -> str:
    """Read the correlation id (mutId / scheduleId) the client assigned."""
    key = {"mutate": "mutId", "schedule": "scheduleId", "cancelSchedule": "scheduleId"}[typ]
    for f in conn.sent:
        d = json.loads(f)
        if d.get("type") == typ:
            return d[key]
    raise AssertionError(f"no frame of type {typ}")


async def test_mutate_resolves_on_mutate_ok():
    conn = FakeConn()
    client = await _connected(conn)
    try:
        task = asyncio.create_task(client.mutate(_insert_txn()))
        await _drain()
        mid = _id(conn, "mutate")
        # StepResult insert shape is ``{"id"}`` (no ``op`` — see http_client tests).
        await conn.deliver('{"type":"mutateOk","mutId":"' + mid + '","results":[{"id":"i1"}]}')
        await _drain()
        results = await asyncio.wait_for(task, 1.0)
        assert results[0] is not None
        assert results[0].model_dump()["id"] == "i1"
    finally:
        await client.close()


async def test_mutate_rejects_on_mutate_err():
    conn = FakeConn()
    client = await _connected(conn)
    try:
        task = asyncio.create_task(client.mutate(_insert_txn()))
        await _drain()
        mid = _id(conn, "mutate")
        await conn.deliver(
            '{"type":"mutateErr","mutId":"'
            + mid
            + '","error":{"code":"NOT_FOUND","message":"no table"}}'
        )
        await _drain()
        with pytest.raises(RtDbError):
            await asyncio.wait_for(task, 1.0)
    finally:
        await client.close()


async def test_inflight_mutate_rejected_on_drop():
    conn = FakeConn()
    client = await _connected(conn)
    try:
        # One in-flight mutate (sent), then a reconnectable drop.
        inflight = asyncio.create_task(client.mutate(_insert_txn()))
        await _drain()
        conn.close_code = 4000
        await conn._inbox.put(None)
        await _drain()
        with pytest.raises(RtDbError):
            await asyncio.wait_for(inflight, 1.0)
    finally:
        await client.close()


async def test_queued_mutate_survives_drop_flushes_on_reconnect():
    conn1 = FakeConn()
    client, _ = make_client(conn1)
    try:
        await client.connect()
        await _drain()
        await conn1.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _drain()
        assert client.status().state is ConnectionState.CONNECTED

        # Swap to a gated connect so the reconnect parks inside _connect (state
        # CONNECTING) while we queue a mutate from a disconnected state.
        conn2 = FakeConn()
        gate = asyncio.Event()

        async def _gated_connect2(url: str) -> FakeConn:
            await gate.wait()
            return conn2

        client._connect = _gated_connect2
        conn1.close_code = 4000
        await conn1._inbox.put(None)
        await _wait_until(lambda: client.status().state is ConnectionState.CONNECTING)

        queued = asyncio.create_task(client.mutate(_insert_txn()))
        await _drain()
        # Not sent yet — the entry is queued (sent=False).
        assert not any('"type":"mutate"' in f for f in conn2.sent)

        # Release the reconnect; the queued frame flushes right after authOk.
        gate.set()
        await _wait_until(lambda: any('"type":"auth"' in f for f in conn2.sent))
        await conn2.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: any('"type":"mutate"' in f for f in conn2.sent))
        mid = _id(conn2, "mutate")
        await conn2.deliver('{"type":"mutateOk","mutId":"' + mid + '","results":[{"id":"i1"}]}')
        results = await asyncio.wait_for(queued, 1.0)
        assert results[0] is not None
        assert results[0].model_dump()["id"] == "i1"
        assert client.status().state is ConnectionState.CONNECTED
    finally:
        await client.close()


async def test_schedule_lifecycle():
    conn = FakeConn()
    client = await _connected(conn)
    try:
        # ScheduleWhen is a pydantic discriminated union (tagged on "type"),
        # not a tuple — build the afterMs variant directly.
        sch = asyncio.create_task(client.schedule(_insert_txn(), AfterMs(ms=100)))
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
    finally:
        await client.close()
