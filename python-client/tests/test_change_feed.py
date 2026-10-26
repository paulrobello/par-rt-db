"""WebSocket change-feed push channel (``RtDbClient.watch_changes``).

No-socket style, mirroring ``tests/test_presence.py``.
"""

import asyncio
import json
from typing import Any

import pytest

from par_rt_db.wire import _ClientFeedSubscribe
from par_rt_db.ws_client import ChangeBatch, ConnectionState, RtDbClient, RtDbError, _PeerClosed

EPOCH = "0a1b2c3d4e5f6071"


class _FakeConn:
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

    def frames(self, type_: str) -> list[dict[str, Any]]:
        out = [json.loads(f) for f in self.sent]
        return [f for f in out if f.get("type") == type_]


async def _const_token() -> str:
    return "tok"


def _op(seq: int) -> dict[str, Any]:
    return {
        "seq": seq,
        "table": "tasks",
        "docId": f"d{seq}",
        "kind": "patch",
        "doc": {"n": seq},
        "ts": 1758300000000 + seq,
    }


def _events(feed_id: str, seqs: list[int], head: int) -> str:
    return json.dumps(
        {
            "type": "feedEvents",
            "feedId": feed_id,
            "feedEpoch": EPOCH,
            "ops": [_op(s) for s in seqs],
            "head": head,
        }
    )


async def _drain(n: int = 16) -> None:
    for _ in range(n):
        await asyncio.sleep(0)


async def _wait_until(pred: object, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():  # type: ignore[operator]
        if loop.time() >= deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(0.005)


async def _connected(conns: list[_FakeConn]) -> RtDbClient:
    """Client whose Nth connect attempt uses ``conns[N]`` (last one repeats)."""
    calls = 0

    async def _connect(url: str) -> _FakeConn:
        nonlocal calls
        conn = conns[min(calls, len(conns) - 1)]
        calls += 1
        return conn

    client = RtDbClient(
        "http://x",
        "db",
        get_token=_const_token,
        connect=_connect,
        heartbeat=10.0,
        backoff_base=0.01,
        backoff_max=0.05,
        random=lambda: 0.5,
    )
    await client.connect()
    await _drain()
    await conns[0].deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    assert client.status().state is ConnectionState.CONNECTED
    return client


async def _reconnect(client: RtDbClient, old: _FakeConn, new: _FakeConn) -> None:
    old.close_code = 4000
    await old._inbox.put(None)
    await _wait_until(lambda: any('"type":"auth"' in f for f in new.sent))
    await new.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _wait_until(lambda: client.status().state is ConnectionState.CONNECTED)


def test_feed_subscribe_omits_defaults() -> None:
    bare = _ClientFeedSubscribe(feed_id="f1").model_dump_json(by_alias=True)
    assert json.loads(bare) == {"type": "feedSubscribe", "feedId": "f1"}
    full = _ClientFeedSubscribe(
        feed_id="f2", since=41, feed_epoch=EPOCH, tables=["tasks", "projects"]
    ).model_dump_json(by_alias=True)
    assert json.loads(full) == {
        "type": "feedSubscribe",
        "feedId": "f2",
        "since": 41,
        "feedEpoch": EPOCH,
        "tables": ["tasks", "projects"],
    }


async def test_subscribe_sends_tables_and_since() -> None:
    conn = _FakeConn()
    client = await _connected([conn])
    try:
        client.watch_changes(tables=["tasks"], since=7, feed_epoch=EPOCH)
        await _wait_until(lambda: conn.frames("feedSubscribe"))
        assert conn.frames("feedSubscribe") == [
            {
                "type": "feedSubscribe",
                "feedId": "feed-1",
                "since": 7,
                "feedEpoch": EPOCH,
                "tables": ["tasks"],
            }
        ]
    finally:
        await client.close()


async def test_yields_batch_then_acks_on_next() -> None:
    conn = _FakeConn()
    client = await _connected([conn])
    try:
        feed = client.watch_changes()
        await _wait_until(lambda: conn.frames("feedSubscribe"))
        await conn.deliver(_events("feed-1", [], 5))
        await conn.deliver(_events("feed-1", [6, 7], 7))
        it = aiter(feed)
        batch = await asyncio.wait_for(anext(it), 2)
        assert isinstance(batch, ChangeBatch)
        assert [op.seq for op in batch.ops] == [6, 7]
        assert batch.feed_epoch == EPOCH and batch.head == 7
        await conn.deliver(_events("feed-1", [8], 8))
        await asyncio.wait_for(anext(it), 2)
        assert conn.frames("feedAck") == [{"type": "feedAck", "feedId": "feed-1", "seq": 7}]
    finally:
        await client.close()


async def test_reconnect_resumes_with_last_seq_and_dedupes_replay() -> None:
    conn1, conn2 = _FakeConn(), _FakeConn()
    client = await _connected([conn1, conn2])
    try:
        feed = client.watch_changes(tables=["tasks"])
        await _wait_until(lambda: conn1.frames("feedSubscribe"))
        assert conn1.frames("feedSubscribe")[0] == {
            "type": "feedSubscribe",
            "feedId": "feed-1",
            "tables": ["tasks"],
        }
        await conn1.deliver(_events("feed-1", [10, 11], 11))
        it = aiter(feed)
        first = await asyncio.wait_for(anext(it), 2)
        assert [op.seq for op in first.ops] == [10, 11]

        await _reconnect(client, conn1, conn2)
        await _wait_until(lambda: conn2.frames("feedSubscribe"))
        assert conn2.frames("feedSubscribe") == [
            {
                "type": "feedSubscribe",
                "feedId": "feed-1",
                "since": 11,
                "feedEpoch": EPOCH,
                "tables": ["tasks"],
            }
        ]

        # At-least-once replay: 10, 11 are dropped, 12 is yielded.
        await conn2.deliver(_events("feed-1", [10, 11, 12], 12))
        second = await asyncio.wait_for(anext(it), 2)
        assert [op.seq for op in second.ops] == [12]

        # A frame holding only replays is swallowed entirely.
        await conn2.deliver(_events("feed-1", [11, 12], 12))
        await conn2.deliver(_events("feed-1", [13], 13))
        third = await asyncio.wait_for(anext(it), 2)
        assert [op.seq for op in third.ops] == [13]
    finally:
        await client.close()


async def test_cursor_expired_raises_and_is_not_retried() -> None:
    conn1, conn2 = _FakeConn(), _FakeConn()
    client = await _connected([conn1, conn2])
    try:
        feed = client.watch_changes(since=3)
        await _wait_until(lambda: conn1.frames("feedSubscribe"))
        await conn1.deliver(
            json.dumps(
                {
                    "type": "feedErr",
                    "feedId": "feed-1",
                    "error": {"code": "CURSOR_EXPIRED", "message": "resync from 0"},
                }
            )
        )
        with pytest.raises(RtDbError) as exc:
            await asyncio.wait_for(anext(aiter(feed)), 2)
        assert exc.value.code.value == "CURSOR_EXPIRED"

        await _reconnect(client, conn1, conn2)
        await _drain(20)
        assert conn2.frames("feedSubscribe") == []
    finally:
        await client.close()


async def test_close_sends_unsubscribe_and_ends_iteration() -> None:
    conn1, conn2 = _FakeConn(), _FakeConn()
    client = await _connected([conn1, conn2])
    try:
        async with client.watch_changes() as feed:
            await _wait_until(lambda: conn1.frames("feedSubscribe"))
        await _wait_until(lambda: conn1.frames("feedUnsubscribe"))
        assert conn1.frames("feedUnsubscribe") == [{"type": "feedUnsubscribe", "feedId": "feed-1"}]
        with pytest.raises(StopAsyncIteration):
            await anext(aiter(feed))

        await _reconnect(client, conn1, conn2)
        await _drain(20)
        assert conn2.frames("feedSubscribe") == []
    finally:
        await client.close()
