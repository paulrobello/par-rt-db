"""ENH-015 presence: wire round-trip + reactive ``RtDbClient`` method tests.

Mirrors ``tests/test_ws_routing.py``'s no-socket style (``FakeConn`` + injected
connect). Wire-shape tests assert the exact camelCase bytes the four clients
agree on (server ``protocol.rs`` is the source of truth).
"""

import asyncio
import json

import pytest
from pydantic import TypeAdapter

from par_rt_db.wire import (
    ClientMessage,
    PresenceMember,
    ServerMessage,
)
from par_rt_db.ws_client import (
    ConnectionState,
    RtDbClient,
    RtDbError,
    _PeerClosed,
)

# --- Wire round-trip: client -> server --------------------------------------


def test_presence_join_omits_state_when_none() -> None:
    """`{type:"presence", room}` — `state` omitted when None (server parity)."""
    adapter = TypeAdapter(ClientMessage)
    msg = adapter.validate_python({"type": "presence", "room": "doc:1"})
    assert msg.model_dump(by_alias=True, mode="json") == {"type": "presence", "room": "doc:1"}


def test_presence_join_includes_state_when_present() -> None:
    """`{type:"presence", room, state}` — state carried when not None."""
    adapter = TypeAdapter(ClientMessage)
    msg = adapter.validate_python({"type": "presence", "room": "doc:1", "state": {"x": 3, "y": 4}})
    assert msg.model_dump(by_alias=True, mode="json") == {
        "type": "presence",
        "room": "doc:1",
        "state": {"x": 3, "y": 4},
    }


def test_presence_state_always_carries_state() -> None:
    adapter = TypeAdapter(ClientMessage)
    msg = adapter.validate_python(
        {"type": "presenceState", "room": "doc:1", "state": {"typing": True}}
    )
    assert msg.model_dump(by_alias=True, mode="json") == {
        "type": "presenceState",
        "room": "doc:1",
        "state": {"typing": True},
    }


def test_leave_presence_is_room_only() -> None:
    adapter = TypeAdapter(ClientMessage)
    msg = adapter.validate_python({"type": "leavePresence", "room": "doc:1"})
    assert msg.model_dump(by_alias=True, mode="json") == {"type": "leavePresence", "room": "doc:1"}


# --- Wire round-trip: server -> client --------------------------------------


def test_presence_snapshot_round_trips_members() -> None:
    """`presenceSnapshot` carries `members: PresenceMember[]` (camelCase)."""
    adapter = TypeAdapter(ServerMessage)
    raw = {
        "type": "presenceSnapshot",
        "room": "doc:1",
        "members": [
            {
                "connectionId": "c1",
                "user": {"kind": "user", "email": "a@b.com", "name": "A"},
                "state": {"x": 1},
            },
            {
                "connectionId": "c2",
                "user": {"kind": "machine"},
                "state": None,
            },
        ],
    }
    msg = adapter.validate_python(raw)
    dumped = msg.model_dump(by_alias=True, mode="json")
    assert dumped["type"] == "presenceSnapshot"
    assert dumped["room"] == "doc:1"
    assert dumped["members"][0] == {
        "connectionId": "c1",
        "user": {"kind": "user", "email": "a@b.com", "name": "A"},
        "state": {"x": 1},
    }
    # state is always present on the wire (None -> null), mirrors the server.
    assert dumped["members"][1] == {
        "connectionId": "c2",
        "user": {"kind": "machine", "email": None, "name": None},
        "state": None,
    }


def test_presence_err_round_trips_envelope() -> None:
    adapter = TypeAdapter(ServerMessage)
    msg = adapter.validate_python(
        {
            "type": "presenceErr",
            "room": "doc:1",
            "error": {"code": "FORBIDDEN", "message": "presence not enabled"},
        }
    )
    assert msg.model_dump(by_alias=True, mode="json") == {
        "type": "presenceErr",
        "room": "doc:1",
        "error": {"code": "FORBIDDEN", "message": "presence not enabled"},
    }


def test_presence_member_rejects_unknown_fields() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PresenceMember.model_validate(
            {"connectionId": "c1", "user": {"kind": "user"}, "state": None, "bogus": 1}
        )


# --- no-socket harness (mirrors tests/test_ws_routing.py) --------------------


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


async def _const_token() -> str:
    return "tok"


def _make_client(conn: _FakeConn) -> RtDbClient:
    async def _connect(url: str) -> _FakeConn:
        return conn

    return RtDbClient(
        "http://x",
        "db",
        get_token=_const_token,
        connect=_connect,
        heartbeat=10.0,
        backoff_base=0.01,
        backoff_max=0.05,
        random=lambda: 0.5,
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


async def _connected(conn: _FakeConn) -> RtDbClient:
    client = _make_client(conn)
    await client.connect()
    await _drain()
    await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
    await _drain()
    assert client.status().state is ConnectionState.CONNECTED
    return client


# --- RtDbClient.presence / update_presence / leave_presence -----------------


async def test_presence_join_sends_frame_when_connected() -> None:
    conn = _FakeConn()
    client = await _connected(conn)
    try:
        client.presence("doc:1")
        await _drain()
        assert any(json.loads(f) == {"type": "presence", "room": "doc:1"} for f in conn.sent)
    finally:
        await client.close()


async def test_presence_join_with_state_carries_state() -> None:
    conn = _FakeConn()
    client = await _connected(conn)
    try:
        client.presence("doc:1", state={"cursor": 5})
        await _drain()
        assert json.loads(next(f for f in conn.sent if '"presence"' in f)) == {
            "type": "presence",
            "room": "doc:1",
            "state": {"cursor": 5},
        }
    finally:
        await client.close()


async def test_presence_snapshot_routes_to_room_handle() -> None:
    conn = _FakeConn()
    client = await _connected(conn)
    try:
        handle = client.presence("doc:1")
        await _drain()
        assert handle.current() is None  # before first snapshot

        await conn.deliver(
            json.dumps(
                {
                    "type": "presenceSnapshot",
                    "room": "doc:1",
                    "members": [
                        {
                            "connectionId": "c1",
                            "user": {"kind": "user", "email": "a@b.com", "name": "A"},
                            "state": {"x": 1},
                        }
                    ],
                }
            )
        )
        await _drain()
        members = handle.current()
        assert members is not None
        assert len(members) == 1
        assert members[0].connection_id == "c1"
        assert members[0].user.email == "a@b.com"
        assert members[0].state == {"x": 1}

        # The async iterator yields the latest member list.
        got = None
        async for value in handle:
            got = value
            break
        assert got is not None and got[0].connection_id == "c1"
    finally:
        await client.close()


async def test_presence_err_sets_handle_error_and_drops_room() -> None:
    conn = _FakeConn()
    client = await _connected(conn)
    try:
        handle = client.presence("doc:1")
        await _drain()
        await conn.deliver(
            json.dumps(
                {
                    "type": "presenceErr",
                    "room": "doc:1",
                    "error": {"code": "FORBIDDEN", "message": "presence not enabled"},
                }
            )
        )
        await _drain()
        err = handle.error()
        assert isinstance(err, RtDbError)
        assert err.message == "presence not enabled"

        # async-for raises the RtDbError (mirrors subscribeErr on Subscription).
        with pytest.raises(RtDbError):
            async for _ in handle:
                pass

        # Room dropped: a reconnect will not re-send the join.
        conn2 = _FakeConn()

        async def _connect2(url: str) -> _FakeConn:
            return conn2

        client._connect = _connect2  # type: ignore[assignment]
        conn.close_code = 4000
        await conn._inbox.put(None)
        await _wait_until(lambda: any('"type":"auth"' in f for f in conn2.sent))
        await conn2.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: client.status().state is ConnectionState.CONNECTED)
        await _drain(20)
        assert not any('"type":"presence"' in f for f in conn2.sent)
    finally:
        await client.close()


async def test_update_presence_sends_presence_state_when_connected() -> None:
    conn = _FakeConn()
    client = await _connected(conn)
    try:
        client.presence("doc:1", state={"typing": False})
        await _drain()
        client.update_presence("doc:1", {"typing": True})
        await _drain()
        assert any(
            json.loads(f) == {"type": "presenceState", "room": "doc:1", "state": {"typing": True}}
            for f in conn.sent
        )
    finally:
        await client.close()


async def test_leave_presence_sends_leave_and_clears_room() -> None:
    conn = _FakeConn()
    client = await _connected(conn)
    try:
        client.presence("doc:1")
        await _drain()
        client.leave_presence("doc:1")
        await _drain()
        assert any(json.loads(f) == {"type": "leavePresence", "room": "doc:1"} for f in conn.sent)
        # Room cleared: a reconnect will not re-send the join.
        conn2 = _FakeConn()

        async def _connect2(url: str) -> _FakeConn:
            return conn2

        client._connect = _connect2  # type: ignore[assignment]
        conn.close_code = 4000
        await conn._inbox.put(None)
        await _wait_until(lambda: any('"type":"auth"' in f for f in conn2.sent))
        await conn2.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: client.status().state is ConnectionState.CONNECTED)
        await _drain(20)
        assert not any('"type":"presence"' in f for f in conn2.sent)
    finally:
        await client.close()


# --- Auth-gating: pre-auth calls buffer and replay on authOk ----------------
#
# Mirrors the ts-client T10 regression: presence()/update_presence()/
# leave_presence() must NOT send before authOk — the join is buffered and
# replayed by _flush_on_auth, exactly how subscribe queues in _subs_by_id.


async def test_pre_auth_presence_buffers_and_replays_on_authOk() -> None:
    conn = _FakeConn()
    client = _make_client(conn)
    try:
        await client.connect()
        await _drain()
        # Pre-auth: state is CONNECTING, not CONNECTED.
        assert client.status().state is ConnectionState.CONNECTING
        client.presence("doc:1", state={"x": 1})
        await _drain()
        # No frame sent yet — buffered until authOk.
        assert not any('"type":"presence"' in f for f in conn.sent)

        # Complete auth; the buffered join replays.
        await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: any('"type":"presence"' in f for f in conn.sent))
        # Exactly one join, with the buffered state.
        joins = [json.loads(f) for f in conn.sent if json.loads(f).get("type") == "presence"]
        assert len(joins) == 1
        assert joins[0] == {"type": "presence", "room": "doc:1", "state": {"x": 1}}
    finally:
        await client.close()


async def test_pre_auth_update_updates_buffered_join_state() -> None:
    """update_presence before auth updates the buffered join's state so the
    replay carries the latest value (not a stale one)."""
    conn = _FakeConn()
    client = _make_client(conn)
    try:
        await client.connect()
        await _drain()
        client.presence("doc:1", state={"v": 1})
        await _drain()
        # Update before auth: the buffered join state moves to v=2.
        client.update_presence("doc:1", {"v": 2})
        await _drain()
        assert not any('"type":"presence"' in f for f in conn.sent)
        assert not any('"type":"presenceState"' in f for f in conn.sent)

        await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: any('"type":"presence"' in f for f in conn.sent))
        joins = [json.loads(f) for f in conn.sent if json.loads(f).get("type") == "presence"]
        assert joins[0]["state"] == {"v": 2}  # latest state, not the stale v=1
        # And no presenceState frame is sent for the pre-auth update.
        assert not any('"type":"presenceState"' in f for f in conn.sent)
    finally:
        await client.close()


async def test_pre_auth_leave_cancels_buffered_join() -> None:
    """leave_presence before auth clears the buffered join so authOk does NOT
    replay it — parity with the ts-client fix."""
    conn = _FakeConn()
    client = _make_client(conn)
    try:
        await client.connect()
        await _drain()
        client.presence("doc:1")
        await _drain()
        client.leave_presence("doc:1")
        await _drain()
        # No frames at all yet (buffered join cancelled, no leave to send pre-auth).
        assert not any('"type":"presence"' in f for f in conn.sent)
        assert not any('"type":"leavePresence"' in f for f in conn.sent)

        await conn.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: client.status().state is ConnectionState.CONNECTED)
        await _drain(20)
        # The buffered join was cancelled: no presence frame on authOk.
        assert not any('"type":"presence"' in f for f in conn.sent)
        assert not any('"type":"leavePresence"' in f for f in conn.sent)
    finally:
        await client.close()


async def test_reconnect_replays_active_presence_join_with_latest_state() -> None:
    conn = _FakeConn()
    client = await _connected(conn)
    try:
        client.presence("doc:1", state={"v": 1})
        await _drain()
        # Update the join state; the cached state moves so a reconnect replays
        # with the latest value.
        client.update_presence("doc:1", {"v": 2})
        await _drain()

        conn2 = _FakeConn()

        async def _connect2(url: str) -> _FakeConn:
            return conn2

        client._connect = _connect2  # type: ignore[assignment]
        conn.close_code = 4000
        await conn._inbox.put(None)
        await _wait_until(lambda: any('"type":"auth"' in f for f in conn2.sent))
        await conn2.deliver('{"type":"authOk","user":{"kind":"machine"}}')
        await _wait_until(lambda: any('"type":"presence"' in f for f in conn2.sent))
        joins = [json.loads(f) for f in conn2.sent if json.loads(f).get("type") == "presence"]
        assert len(joins) == 1
        assert joins[0]["state"] == {"v": 2}
    finally:
        await client.close()


async def test_presence_handle_unsubscribe_does_not_leave_room() -> None:
    """A handle drop only removes the listener; the room stays joined until
    leave_presence is called (parity with ts-client/rust-client)."""
    conn = _FakeConn()
    client = await _connected(conn)
    try:
        h1 = client.presence("doc:1")
        await _drain()
        h2 = client.presence("doc:1")  # second listener on the same room
        await _drain()
        # Only one join frame for two handles (room joined once).
        joins = [f for f in conn.sent if json.loads(f).get("type") == "presence"]
        assert len(joins) == 1

        h1.unsubscribe()
        await _drain()
        # No leave frame — the room is still joined (h2 + explicit leave contract).
        assert not any('"type":"leavePresence"' in f for f in conn.sent)

        # A snapshot still routes to the remaining handle.
        await conn.deliver(
            json.dumps(
                {
                    "type": "presenceSnapshot",
                    "room": "doc:1",
                    "members": [
                        {
                            "connectionId": "c1",
                            "user": {"kind": "user"},
                            "state": {"k": 1},
                        }
                    ],
                }
            )
        )
        await _drain()
        h2_members = h2.current()
        assert h2_members is not None
        assert h2_members[0].connection_id == "c1"
        # The unsubscribed handle is closed: __anext__ raises StopAsyncIteration.
        # (``async for`` swallows StopAsyncIteration as loop-end, so test the hook
        # directly rather than wrapping the loop in pytest.raises.)
        h1_it = h1.__aiter__()
        with pytest.raises(StopAsyncIteration):
            await h1_it.__anext__()
    finally:
        await client.close()


# --- In-memory harness presence (mirrors rust-client/ts-client) -------------


def test_in_memory_presence_join_fires_initial_snapshot_with_self() -> None:
    from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions

    client = InMemoryRtDbClient(InMemoryRtDbClientOptions(connection_id="c1"))
    snaps: list[list] = []
    handle = client.presence("room:a", {"x": 1}, snaps.append)
    try:
        # Initial snapshot fires synchronously with just this connection.
        assert len(snaps) == 1
        assert len(snaps[0]) == 1
        assert snaps[0][0].connection_id == "c1"
        assert snaps[0][0].state == {"x": 1}
    finally:
        handle.unsubscribe()


def test_in_memory_presence_update_broadcasts_new_state() -> None:
    from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions

    client = InMemoryRtDbClient(InMemoryRtDbClientOptions(connection_id="c1"))
    snaps: list[list] = []
    handle = client.presence("room:a", {"v": 1}, snaps.append)
    try:
        snaps.clear()
        client.update_presence("room:a", {"v": 2})
        assert len(snaps) == 1
        assert snaps[0][0].state == {"v": 2}
    finally:
        handle.unsubscribe()


def test_in_memory_presence_update_noop_for_unjoined_room() -> None:
    from par_rt_db.in_memory import InMemoryRtDbClient

    client = InMemoryRtDbClient()
    snaps: list[list] = []
    handle = client.presence("room:a", None, snaps.append)
    try:
        snaps.clear()
        client.update_presence("room:b", {"v": 9})  # never joined
        assert snaps == []
    finally:
        handle.unsubscribe()


def test_in_memory_presence_leave_removes_member_and_drops_listeners() -> None:
    from par_rt_db.in_memory import (
        InMemoryRtDbClient,
        InMemoryRtDbClientOptions,
        PresenceRooms,
    )

    rooms = PresenceRooms()
    c1 = InMemoryRtDbClient(InMemoryRtDbClientOptions(connection_id="c1", presence_rooms=rooms))
    c2 = InMemoryRtDbClient(InMemoryRtDbClientOptions(connection_id="c2", presence_rooms=rooms))
    c1_snaps: list[int] = []
    h1 = c1.presence("room:a", None, lambda members: c1_snaps.append(len(members)))
    try:
        # c2 joins -> c1 sees 2 members.
        c2.presence("room:a", None, lambda _: None)
        assert c1_snaps == [1, 2]

        # c1 leaves: its listener is dropped, the fan-out goes to remaining
        # listeners only (c2). c1's snapshot history is unchanged by the leave.
        c1.leave_presence("room:a")
        assert c1_snaps == [1, 2]  # no further callback fire on c1

        # The backing no longer lists c1.
        assert [m.connection_id for m in rooms.snapshot("room:a")] == ["c2"]

        # Idempotent: a second leave is a no-op (c1 already gone).
        c1.leave_presence("room:a")
    finally:
        h1.unsubscribe()


def test_in_memory_presence_two_clients_on_shared_rooms_see_each_other() -> None:
    from par_rt_db.in_memory import (
        InMemoryRtDbClient,
        InMemoryRtDbClientOptions,
        PresenceRooms,
    )

    rooms = PresenceRooms()
    a = InMemoryRtDbClient(InMemoryRtDbClientOptions(connection_id="a", presence_rooms=rooms))
    b = InMemoryRtDbClient(InMemoryRtDbClientOptions(connection_id="b", presence_rooms=rooms))
    a_snaps: list[list] = []
    b_snaps: list[list] = []
    a_handle = a.presence("room:1", {"role": "editor"}, a_snaps.append)
    try:
        # A sees only itself on join.
        assert [m.connection_id for m in a_snaps[-1]] == ["a"]
        b_handle = b.presence("room:1", {"role": "viewer"}, b_snaps.append)
        try:
            # B's join fans out to A as well (shared backing): A now sees both.
            assert {m.connection_id for m in a_snaps[-1]} == {"a", "b"}
            # B's initial snapshot includes both members.
            assert {m.connection_id for m in b_snaps[-1]} == {"a", "b"}

            b.update_presence("room:1", {"role": "typist"})
            # A observes B's updated state.
            b_member = next(m for m in a_snaps[-1] if m.connection_id == "b")
            assert b_member.state == {"role": "typist"}
        finally:
            b_handle.unsubscribe()
        b.leave_presence("room:1")
        # A is alone again after B leaves.
        assert [m.connection_id for m in a_snaps[-1]] == ["a"]
    finally:
        a_handle.unsubscribe()
