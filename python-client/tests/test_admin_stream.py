"""Tests for the ``/admin/stream`` op-feed mirror (``stream_admin`` /
``astream_admin``) — mock streams only, no live server. The connect seams
(``_connect_admin_stream`` / ``_aconnect_admin_stream``) are monkeypatched to
hand out scripted fake sockets, and the reconnect sleep is recorded instead
of waited on. Mirrors the rust/ts semantics: unknown-kind frames are
skipped, transport drops reconnect (with a duplicate replay), a rejected
upgrade and a mid-stream 4401 close are terminal without retry."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator, Generator

import pytest

import par_rt_db.admin as admin_mod
from par_rt_db.admin_models import AdminGaugesFrame, AdminOpFrame
from par_rt_db.errors import ErrorCode, RtDbError


def _op_frame(doc_id: str, seq: int = 1) -> str:
    """A wire-shaped op frame."""
    return json.dumps(
        {
            "kind": "op",
            "event": {
                "db": "d",
                "table": "items",
                "docId": doc_id,
                "kind": "insert",
                "ts": 1,
                "owner": None,
                "seq": seq,
                "feedEpoch": "test-epoch",
            },
        }
    )


def _gauges_frame() -> str:
    """A wire-shaped gauges frame with every required MetricsSnapshot field."""
    return json.dumps(
        {
            "kind": "gauges",
            "gauges": {
                "queriesTotal": 1,
                "mutationsTotal": 2,
                "uploadsTotal": 0,
                "wsConnections": 0,
                "activeSubscriptions": 0,
                "poolSize": 1,
                "poolIdle": 1,
                "uptimeSeconds": 1,
                "queryLatency": {"p50": 1, "p95": 2, "p99": 3},
                "mutateLatency": {"p50": 1, "p95": 2, "p99": 3},
                "subscribeLatency": {"p50": 1, "p95": 2, "p99": 3},
            },
        }
    )


class _FakeWS:
    """Mock sync stream: yields scripted frames, then optionally raises."""

    def __init__(self, frames: list[str], exc: BaseException | None = None) -> None:
        self.frames = list(frames)
        self.exc = exc
        self.closed = False

    def __iter__(self) -> Generator[str, None, None]:
        yield from self.frames
        if self.exc is not None:
            raise self.exc

    def close(self) -> None:
        self.closed = True


class _FakeAWS:
    """Mock async stream twin."""

    def __init__(self, frames: list[str], exc: BaseException | None = None) -> None:
        self.frames = list(frames)
        self.exc = exc
        self.closed = False

    def __aiter__(self) -> _FakeAWS:
        return self

    async def __anext__(self) -> str:
        if self.frames:
            return self.frames.pop(0)
        if self.exc is not None:
            raise self.exc
        raise StopAsyncIteration

    async def close(self) -> None:
        self.closed = True


# --- parse / url unit tests ---------------------------------------------------


def test_parse_admin_stream_frame_shapes() -> None:
    op = admin_mod._parse_admin_stream_frame(_op_frame("doc-1"))
    assert isinstance(op, AdminOpFrame)
    assert op.event.doc_id == "doc-1"  # camelCase wire → snake_case attr
    gauges = admin_mod._parse_admin_stream_frame(_gauges_frame())
    assert isinstance(gauges, AdminGaugesFrame)
    # Unknown kind / malformed JSON / non-string / incomplete payload: skipped.
    assert admin_mod._parse_admin_stream_frame('{"kind":"futureKind","x":1}') is None
    assert admin_mod._parse_admin_stream_frame("not json") is None
    assert admin_mod._parse_admin_stream_frame(None) is None
    assert admin_mod._parse_admin_stream_frame('{"kind":"op"}') is None
    assert admin_mod._parse_admin_stream_frame('{"kind":"gauges","gauges":{}}') is None
    # Unknown keys on a known kind are ignored, not fatal (lenient base).
    padded = _gauges_frame()[:-1] + ',"future":1}'
    assert isinstance(admin_mod._parse_admin_stream_frame(padded), AdminGaugesFrame)


def test_admin_stream_url_building() -> None:
    assert admin_mod._admin_stream_url("http://s:8300") == "ws://s:8300/admin/stream"
    assert (
        admin_mod._admin_stream_url("https://s/", "db1", "my table")
        == "wss://s/admin/stream?db=db1&table=my+table"
    )


# --- sync generator -----------------------------------------------------------


def test_stream_admin_skips_unknown_and_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conns = [
        # Connection 1: op, unknown kind (must never surface), gauges, then a
        # clean end (the reconnect path).
        _FakeWS([_op_frame("doc-1"), '{"kind":"futureKind","x":1}', _gauges_frame()]),
        # Connection 2: the server replays after the blip.
        _FakeWS([_op_frame("doc-2")]),
    ]
    opened: list[_FakeWS] = []
    opens: list[tuple[str, str]] = []

    def fake_connect(url: str, admin_key: str) -> _FakeWS:
        opens.append((url, admin_key))
        if not conns:
            raise AssertionError("script exhausted: unexpected reconnect")
        opened.append(conn := conns.pop(0))
        return conn

    sleeps: list[float] = []
    monkeypatch.setattr(admin_mod, "_connect_admin_stream", fake_connect)
    monkeypatch.setattr(admin_mod.time, "sleep", sleeps.append)

    client = admin_mod.RtDbAdminClient("http://s.test", "k")
    gen = client.stream_admin(db="mydb")
    frames = [next(gen), next(gen), next(gen)]

    assert [f.kind for f in frames] == ["op", "gauges", "op"]  # unknown skipped
    assert isinstance(frames[0], AdminOpFrame) and frames[0].event.doc_id == "doc-1"
    assert isinstance(frames[2], AdminOpFrame) and frames[2].event.doc_id == "doc-2"
    assert len(opens) == 2  # reconnect happened
    assert opens[0][0] == "ws://s.test/admin/stream?db=mydb"
    assert opens[0][1] == "k"
    assert len(sleeps) == 1  # one backoff sleep before the reconnect

    assert isinstance(gen, Generator)
    gen.close()  # consumer break → socket closed, no further reconnect
    assert opened[-1].closed


def test_stream_admin_rejected_handshake_raises_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opens: list[str] = []

    def fake_connect(url: str, admin_key: str) -> _FakeWS:
        opens.append(url)
        raise RtDbError(ErrorCode.UNAUTHORIZED, "admin stream upgrade rejected with status 401")

    monkeypatch.setattr(admin_mod, "_connect_admin_stream", fake_connect)
    client = admin_mod.RtDbAdminClient("http://s.test", "k")
    gen = client.stream_admin()
    with pytest.raises(RtDbError) as ei:
        next(gen)
    assert ei.value.code == ErrorCode.UNAUTHORIZED
    assert len(opens) == 1  # a rejected credential is never retried


def test_stream_admin_4401_close_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("websockets")
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close

    conns = [_FakeWS([_op_frame("doc-1")], exc=ConnectionClosedError(Close(4401, "revoked"), None))]
    opened: list[_FakeWS] = []
    opens: list[str] = []

    def fake_connect(url: str, admin_key: str) -> _FakeWS:
        opens.append(url)
        if not conns:
            raise AssertionError("script exhausted: unexpected reconnect")
        opened.append(conn := conns.pop(0))
        return conn

    monkeypatch.setattr(admin_mod, "_connect_admin_stream", fake_connect)
    monkeypatch.setattr(admin_mod.time, "sleep", lambda s: None)
    client = admin_mod.RtDbAdminClient("http://s.test", "k")
    gen = client.stream_admin()
    first = next(gen)
    assert isinstance(first, AdminOpFrame) and first.event.doc_id == "doc-1"
    with pytest.raises(RtDbError) as ei:
        next(gen)
    assert ei.value.code == ErrorCode.UNAUTHORIZED
    assert opened[-1].closed
    assert len(opens) == 1  # no reconnect after the credential is revoked


# --- async generator ----------------------------------------------------------


async def test_astream_admin_skips_unknown_and_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conns = [
        _FakeAWS([_op_frame("doc-1"), '{"kind":"futureKind","x":1}', _gauges_frame()]),
        _FakeAWS([_op_frame("doc-2")]),
    ]
    opened: list[_FakeAWS] = []
    opens: list[str] = []

    async def fake_aconnect(url: str, admin_key: str) -> _FakeAWS:
        opens.append(url)
        if not conns:
            raise AssertionError("script exhausted: unexpected reconnect")
        opened.append(conn := conns.pop(0))
        return conn

    sleeps: list[float] = []

    async def record_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(admin_mod, "_aconnect_admin_stream", fake_aconnect)
    monkeypatch.setattr("asyncio.sleep", record_sleep)

    client = admin_mod.AsyncRtDbAdminClient("http://s.test", "k")
    agen = client.astream_admin()
    frames = [await agen.__anext__(), await agen.__anext__(), await agen.__anext__()]
    assert [f.kind for f in frames] == ["op", "gauges", "op"]  # unknown skipped
    assert isinstance(frames[2], AdminOpFrame) and frames[2].event.doc_id == "doc-2"
    assert len(opens) == 2  # reconnect happened

    assert isinstance(agen, AsyncGenerator)
    await agen.aclose()  # consumer break → socket closed
    assert opened[-1].closed


async def test_astream_admin_rejected_handshake_raises_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opens: list[str] = []

    async def fake_aconnect(url: str, admin_key: str) -> _FakeAWS:
        opens.append(url)
        raise RtDbError(ErrorCode.UNAUTHORIZED, "admin stream upgrade rejected with status 401")

    monkeypatch.setattr(admin_mod, "_aconnect_admin_stream", fake_aconnect)
    client = admin_mod.AsyncRtDbAdminClient("http://s.test", "k")
    agen = client.astream_admin()
    with pytest.raises(RtDbError) as ei:
        await agen.__anext__()
    assert ei.value.code == ErrorCode.UNAUTHORIZED
    assert len(opens) == 1


async def test_astream_admin_4401_close_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("websockets")
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close

    conns = [
        _FakeAWS([_op_frame("doc-1")], exc=ConnectionClosedError(Close(4401, "revoked"), None))
    ]
    opened: list[_FakeAWS] = []
    opens: list[str] = []

    async def fake_aconnect(url: str, admin_key: str) -> _FakeAWS:
        opens.append(url)
        if not conns:
            raise AssertionError("script exhausted: unexpected reconnect")
        opened.append(conn := conns.pop(0))
        return conn

    monkeypatch.setattr(admin_mod, "_aconnect_admin_stream", fake_aconnect)
    client = admin_mod.AsyncRtDbAdminClient("http://s.test", "k")
    agen = client.astream_admin()
    first = await agen.__anext__()
    assert isinstance(first, AdminOpFrame) and first.event.doc_id == "doc-1"
    with pytest.raises(RtDbError) as ei:
        await agen.__anext__()
    assert ei.value.code == ErrorCode.UNAUTHORIZED
    assert opened[-1].closed
    assert len(opens) == 1  # no reconnect after the credential is revoked
