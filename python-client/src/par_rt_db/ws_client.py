"""Reactive WebSocket client for par-rt-db (the ``[ws]`` extra).

Async client over the ``/sync`` endpoint: one multiplexed connection, live
subscriptions (``async for value in client.subscribe(query)``), at-most-once
mutations, and schedule ops. Mirrors ``ts-client/src/client.ts`` and
``rust-client/src/ws.rs``. ``websockets`` is imported lazily inside the default
``connect`` factory so this module imports without the ``[ws]`` extra installed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random as _random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

from pydantic import TypeAdapter

from .errors import ErrorCode, RtDbError
from .mutation import StepResult, Transaction
from .optimistic import project as _project_optimistic
from .query import _dump_query, _terminal_of, parse_result
from .wire import (
    AuthedUser,
    PresenceMember,
    ScheduleInfo,
    ScheduleWhen,
    ServerMessage,
    _ClientCancelSchedule,
    _ClientLeavePresence,
    _ClientListSchedules,
    _ClientMutate,
    _ClientPauseSchedule,
    _ClientPresence,
    _ClientPresenceState,
    _ClientResumeSchedule,
    _ClientSchedule,
)

_logger = logging.getLogger(__name__)


def _sync_url(url: str) -> str:
    """Flip http(s)→ws(s), strip trailing slashes, append ``/sync``."""
    u = url.strip()
    if u.startswith("https://"):
        u = "wss://" + u[len("https://") :]
    elif u.startswith("http://"):
        u = "ws://" + u[len("http://") :]
    return u.rstrip("/") + "/sync"


def _canonical_key(query_dict: dict[str, Any]) -> str:
    """Stable dedup key for a query's wire dict (order-independent)."""
    return json.dumps(query_dict, sort_keys=True, separators=(",", ":"))


def _backoff_delay(attempt: int, base: float, max_delay: float, rand: float) -> float:
    """Jittered exponential backoff: ``min(max, base * 2**attempt) * (0.5 + rand*0.5)``."""
    raw = min(max_delay, base * (2**attempt))
    return raw * (0.5 + rand * 0.5)


# --- Connection core: driver, handshake, reconnect, heartbeat -----------------

_SERVER = TypeAdapter(ServerMessage)
_STEP_RESULT_ADAPTER = TypeAdapter(StepResult)

_AUTH_FAILED_CODE = 4401
_AUTH_DEADLINE = 15.0


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
    """The socket surface the driver depends on. ``FakeConn`` in tests; ``_RealConn`` in prod."""

    async def send(self, data: str) -> None: ...
    async def recv(self) -> str: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class _PeerClosed(Exception):
    """Translated socket-close: carries the peer's close code."""

    def __init__(self, code: int, reason: str) -> None:
        super().__init__(f"peer closed: {code} {reason}")
        self.code = code
        self.reason = reason


@dataclass
class _Sub:
    """Internal per-shape subscription state, shared by all ``Subscription`` handles.

    ``refcount`` tracks live handles; ``cond`` wakes the async iterator. ``value``/
    ``error``/``version`` are mutated under ``cond`` by the inbound-message handlers
    (which run inside the driver's read loop); the iterator re-checks ``version``
    under the cond before awaiting so a wake between reads is never missed.
    """

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
    cond: asyncio.Condition = field(default_factory=asyncio.Condition)
    # Optimistic-overlay bookkeeping (only read when the client is constructed
    # with ``optimistic_updates=True``). ``query`` is the wire dict (the
    # projection input); ``server_last`` is the raw authoritative result (the
    # projection base, updated on each ``queryUpdate``); ``optimistic_active`` is
    # true while an overlay is currently covering ``value``.
    query: dict[str, Any] = field(default_factory=dict)
    server_last: Any = None
    optimistic_active: bool = False


@dataclass
class _MutPending:
    """One in-flight or queued mutation, keyed by its correlation id (``mut-{n}``)."""

    future: asyncio.Future
    frame: str
    id: str = ""  # the mutId correlation key; ``_reject_inflight`` pops by this
    sent: bool = False  # True once the frame is on the wire (in-flight)


@dataclass
class _SchedPending:
    """One in-flight or queued schedule op, keyed by its correlation id (``sch-{n}``)."""

    future: asyncio.Future
    frame: str
    id: str = ""  # the scheduleId correlation key; popped by this on drop
    kind: str = ""  # "schedule" | "cancel" | "pause" | "resume" | "list"
    sent: bool = False


@dataclass
class _PresenceRoom:
    """Internal per-room presence state (ENH-015), shared by every ``Presence``
    handle on that room. Mirrors ``_Sub``: ``cond`` wakes the async iterator;
    ``members``/``error``/``version`` mutate under ``cond`` from the inbound
    handlers. ``join_state`` is the latest state this connection advertised in
    the room — the source of truth for reconnect replay (a ``presenceState``
    update advances it so the replay carries the freshest value, not the stale
    join value). ``handle_count`` is the live ``Presence`` handle count; a room
    with no handles and no explicit ``leave_presence`` stays joined (parity with
    ts-client/rust-client), but ``leave_presence`` clears it."""

    room: str
    join_state: Any | None = None
    members: list[PresenceMember] | None = None
    error: RtDbError | None = None
    version: int = 0
    closed: bool = False
    handle_count: int = 0
    cond: asyncio.Condition = field(default_factory=asyncio.Condition)


class RtDbClient:
    """Reactive par-rt-db client.

    One background task (``_drive``) owns the socket: it opens the connection,
    handshakes, and runs a read loop + heartbeat per epoch. Reconnectable closes
    back off and reopen; an auth-failure close (4401) is terminal. ``close()``
    cancels the driver and sets state ``CLOSED``.
    """

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
        optimistic_updates: bool = False,
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
        self._optimistic = optimistic_updates

        self._state = ConnectionState.IDLE
        self._user: AuthedUser | None = None
        self._generation = 0  # bumped on every (re)open and on close()
        self._closed = False
        self._attempt = 0
        self._task: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self._ws: Connection | None = None
        self._last_pong = 0.0
        # Populated by Tasks 3-4 (subscriptions, mutations, schedules):
        self._subs_by_key: dict[str, _Sub] = {}
        self._subs_by_id: dict[str, _Sub] = {}
        self._counter = 0
        self._pending_mut: dict[str, Any] = {}
        self._pending_sched: dict[str, Any] = {}
        # Reverse index (mut_id -> query_ids) for optimistic-overlay rollback;
        # only populated when ``optimistic_updates`` is on.
        self._overlays: dict[str, set[str]] = {}
        # ENH-015 presence: one entry per joined room, keyed by room name.
        self._presence_by_room: dict[str, _PresenceRoom] = {}

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
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
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
                await self._sleep(0)  # yield; caller reconnects via connect()
                continue
            gen = self._generation
            await self._epoch(token, gen)

    async def _epoch(self, token: str, gen: int) -> None:
        self._set_state(ConnectionState.CONNECTING)
        try:
            ws = await self._connect(self._url)
        except Exception:
            # Broad catch is intentional: any failure to establish the socket
            # (DNS, TCP, TLS, timeout, handshake) is a reconnect signal, not a
            # crash. Log at info (not error) because reconnects are expected
            # under transient network conditions; logger.exception preserves the
            # traceback for debugging (QA-005).
            _logger.exception("ws connect failed; scheduling reconnect (gen=%s)", gen)
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
            self._set_state(ConnectionState.CONNECTED)
            self._attempt = 0
            self._last_pong = self._now()
            await self._flush_on_auth()
            await self._run_session(ws, gen)
        except _PeerClosed as e:
            await self._on_peer_closed(e.code, gen)
        except Exception:
            # Broad catch is intentional: a transient decode error, network
            # hiccup, or unexpected server frame should trigger reconnect
            # rather than tear down the client. logger.exception surfaces the
            # traceback so a real defect isn't silently swallowed (QA-005).
            _logger.exception("ws session error; scheduling reconnect (gen=%s)", gen)
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
            # Tolerate non-auth frames during the handshake window (e.g. pong).
            self._dispatch(msg)

    async def _run_session(self, ws: Connection, gen: int) -> None:
        self._last_pong = self._now()
        reader = asyncio.create_task(self._read_loop(ws, gen))
        heartbeat = asyncio.create_task(self._heartbeat_loop(ws, gen))
        code = 1000
        try:
            done, pending = await asyncio.wait(
                {reader, heartbeat}, return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            for t in done:
                code = t.result() if not t.cancelled() else code
        finally:
            # Guarantee no leaked subtasks on any exit, including close() cancellation.
            for t in (reader, heartbeat):
                if not t.done():
                    t.cancel()
            for t in (reader, heartbeat):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
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
            with contextlib.suppress(_PeerClosed):
                await ws.send(frame)  # the read loop drives reconnection

    async def _flush_on_auth(self) -> None:
        # Re-establish every active query, then flush queued mutations/schedules.
        # Maps are empty in Task 2; the loops simply don't execute.
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
        # ENH-015: replay one join per joined room, using the latest join_state
        # (a pre-auth update_presence advances it, so the replay stays fresh).
        for room in list(self._presence_by_room):
            await self._send(_presence_join_frame(room, self._presence_by_room[room].join_state))

    # --- dispatch (forward-compatible; Tasks 3-4 fill the maps) --------

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
        elif tag == "presenceSnapshot":
            self._on_presence_snapshot(msg)
        elif tag == "presenceErr":
            self._on_presence_err(msg)
        # authOk/authErr are handled in _await_auth; unknown tags ignored.

    # --- subscriptions (Task 3) ----------------------------------------

    def subscribe(self, query: Any, *, model: type = dict) -> Subscription:
        """Register a live query and return a ``Subscription`` handle.

        Identical query shapes (by canonical wire dict) dedup to a single
        server-side subscription via refcount. When connected, the subscribe
        frame is sent immediately (scheduled on the loop); otherwise it is
        flushed on the next ``authOk`` via ``_flush_on_auth``.
        """
        from .query import TableQuery

        built = query.build() if isinstance(query, TableQuery) else query
        qd = _dump_query(built)
        key = _canonical_key(qd)
        terminal = _terminal_of(built)
        sub = self._subs_by_key.get(key)
        if sub is None:
            self._counter += 1
            qid = f"sub-{self._counter}"
            from .wire import _ClientSubscribe

            frame = _ClientSubscribe(query_id=qid, query=qd).model_dump_json(by_alias=True)
            sub = _Sub(query_id=qid, key=key, frame=frame, terminal=terminal, model=model, query=qd)
            self._subs_by_key[key] = sub
            self._subs_by_id[qid] = sub
            if self._state is ConnectionState.CONNECTED:
                sub.subscribed = True
                asyncio.get_running_loop().create_task(self._send(frame))
        sub.refcount += 1
        return Subscription(self, sub)

    def _decref(self, key: str) -> None:
        """Drop one handle on a subscription; send ``unsubscribe`` when the last leaves."""
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
        # Reconcile: the authoritative result supersedes any in-flight overlay;
        # record it as the new projection base (raw — pre-model-validation).
        sub.server_last = msg.result
        sub.optimistic_active = False
        sub.value = parse_result(sub.model, sub.terminal, msg.result)
        sub.version += 1
        self._notify(sub)

    def _on_subscribe_err(self, msg: Any) -> None:
        sub = self._subs_by_id.get(msg.query_id)
        if sub is None:
            return
        sub.error = RtDbError.from_envelope(msg.error.model_dump())
        # The shape is dead: drop it so a reconnect never re-subscribes it.
        self._subs_by_key.pop(sub.key, None)
        self._subs_by_id.pop(sub.query_id, None)
        self._notify(sub)

    @staticmethod
    def _notify(sub: _Sub) -> None:
        """Wake any async iterator parked on ``sub.cond``.

        Runs inside the driver's read loop (no iterator awaits there), so the
        wake is scheduled as a task that acquires the cond and ``notify_all``s.
        """

        async def _wake() -> None:
            async with sub.cond:
                sub.cond.notify_all()

        # No running loop (defensive): the iterator's version check still
        # observes the new value/error on its next acquire.
        with contextlib.suppress(RuntimeError):
            asyncio.get_running_loop().create_task(_wake())

    # --- mutations + schedule ops (Task 4) -----------------------------
    #
    # At-most-once: each call registers a future keyed by its correlation id
    # (``mut-{n}`` / ``sch-{n}``). When CONNECTED the frame is sent immediately
    # and the entry is marked ``sent=True`` (in-flight); when not connected the
    # entry stays ``sent=False`` (queued) and ``_flush_on_auth`` sends it on the
    # next ``authOk``. A reconnectable drop rejects only in-flight entries; a
    # ``close()`` rejects all of them.

    async def mutate(
        self, txn: Transaction, *, idempotency_key: str | None = None
    ) -> list[StepResult]:
        self._counter += 1
        mid = f"mut-{self._counter}"
        txn_dict = txn.model_dump(by_alias=True, mode="json")
        frame = _ClientMutate(
            mut_id=mid,
            idempotency_key=idempotency_key,
            txn=txn_dict,
        ).model_dump_json(by_alias=True)
        if self._optimistic:
            self._apply_optimistic(mid, txn_dict)
        fut = asyncio.get_running_loop().create_future()
        self._pending_mut[mid] = _MutPending(fut, frame, id=mid)
        await self._dispatch_send(mid, self._pending_mut)
        return await fut

    async def _dispatch_send(self, key: str, table: dict[str, Any]) -> None:
        entry = table[key]
        if self._state is ConnectionState.CONNECTED:
            await self._send(entry.frame)
            entry.sent = True
        # else: queued; ``_flush_on_auth`` sends it on the next authOk.

    async def schedule(self, txn: Transaction, when: ScheduleWhen) -> str:
        return await self._sched_op("schedule", txn=txn, when=when)  # type: ignore[return-value]

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
        fut = asyncio.get_running_loop().create_future()
        self._pending_sched[sid] = _SchedPending(fut, frame, id=sid, kind=kind)
        await self._dispatch_send(sid, self._pending_sched)
        return await fut

    def _on_mutate_ok(self, msg: Any) -> None:
        mp = self._pending_mut.pop(msg.mut_id, None)
        if mp is not None and not mp.future.done():
            mp.future.set_result([_STEP_RESULT_ADAPTER.validate_python(r) for r in msg.results])
        # No revert: the reconciling queryUpdate(s) arrive and supersede any
        # overlay. Just drop the reverse-index entry — those overlays are no
        # longer rollback-eligible.
        self._overlays.pop(msg.mut_id, None)

    def _on_mutate_err(self, msg: Any) -> None:
        mp = self._pending_mut.pop(msg.mut_id, None)
        if mp is not None and not mp.future.done():
            mp.future.set_exception(RtDbError.from_envelope(msg.error.model_dump()))
        self._revert_overlays_for(msg.mut_id)

    def _on_sched(self, msg: Any) -> None:
        sp = self._pending_sched.pop(msg.schedule_id, None)
        if sp is None or sp.future.done():
            return
        tag = _tag(msg)
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
                env = (
                    msg.error.model_dump()
                    if msg.error is not None
                    else {"code": "INTERNAL", "message": "schedule ack failed"}
                )
                sp.future.set_exception(RtDbError.from_envelope(env))

    # --- presence (ENH-015) -------------------------------------------
    #
    # Join a presence room, broadcast state updates, and leave. Mirrors how
    # ``subscribe`` gates sends on the auth state: the join is recorded locally
    # (in ``_presence_by_room``) and the wire frame is sent ONLY when CONNECTED.
    # A pre-auth call buffers the join so ``_flush_on_auth`` replays it on the
    # next ``authOk`` — exactly how ``_subs_by_id`` replays ``subscribe`` frames
    # (parity with the ts-client fix, T10). One room = one connection-side join;
    # multiple ``Presence`` handles on the same room share one ``_PresenceRoom``.

    def presence(self, room: str, state: Any | None = None) -> Presence:
        """Join presence room ``room`` with optional initial ``state`` and return
        a :class:`Presence` handle. The first ``presenceSnapshot`` (the server
        sends one on join) resolves the handle's ``current()`` from ``None``.

        Identical rooms share one server-side join via handle count. The join is
        sent only when authenticated; otherwise it buffers and ``_flush_on_auth``
        replays it on ``authOk``. Handle ``.unsubscribe()`` only detaches the
        listener; call :meth:`leave_presence` to actually leave the room.
        """
        rm = self._presence_by_room.get(room)
        if rm is None:
            rm = _PresenceRoom(room=room, join_state=state)
            self._presence_by_room[room] = rm
            if self._state is ConnectionState.CONNECTED:
                asyncio.get_running_loop().create_task(
                    self._send(_presence_join_frame(room, state))
                )
        elif state is not None:
            # Refresh the cached join state so a reconnect replays the freshest
            # value (parity with rust-client/ts-client on re-join).
            rm.join_state = state
        rm.handle_count += 1
        return Presence(self, rm)

    def update_presence(self, room: str, state: Any, ttl_ms: int | None = None) -> None:
        """Broadcast updated ``state`` for this connection in ``room``. Also
        advances the cached join state so a reconnect/``authOk`` replay carries
        the latest value. The wire frame is sent ONLY when authenticated — a
        pre-auth update just advances the cached state of the buffered join.

        ``ttl_ms`` (ENH-015 follow-up) arms a per-state expiry on the server
        (forwarded as ``ttlMs``; omitted when ``None``); the server clears this
        connection's ``state`` to ``null`` ``ttl_ms`` after the last refresh and
        the member stays listed. ttl rides on ``update_presence`` only — the join
        frame is unchanged."""
        rm = self._presence_by_room.get(room)
        if rm is None:
            return  # not joined — mirrors the live server ignoring a non-member
        rm.join_state = state
        if self._state is ConnectionState.CONNECTED:
            frame = _ClientPresenceState(room=room, state=state, ttl_ms=ttl_ms).model_dump_json(
                by_alias=True
            )
            asyncio.get_running_loop().create_task(self._send(frame))

    def leave_presence(self, room: str) -> None:
        """Leave presence room ``room``: drops local state and (when
        authenticated) sends ``leavePresence``. Local state is cleared regardless
        of auth so a buffered pre-auth join does not replay after the caller has
        already left — parity with the ts-client fix (T10)."""
        rm = self._presence_by_room.pop(room, None)
        if rm is None:
            return
        rm.closed = True
        self._notify_presence(rm)
        if self._state is ConnectionState.CONNECTED:
            asyncio.get_running_loop().create_task(
                self._send(_ClientLeavePresence(room=room).model_dump_json(by_alias=True))
            )

    def _on_presence_snapshot(self, msg: Any) -> None:
        rm = self._presence_by_room.get(msg.room)
        if rm is None:
            return
        rm.members = list(msg.members)
        rm.version += 1
        self._notify_presence(rm)

    def _on_presence_err(self, msg: Any) -> None:
        rm = self._presence_by_room.get(msg.room)
        if rm is None:
            return
        rm.error = RtDbError.from_envelope(msg.error.model_dump())
        # The join is dead: drop it so a reconnect never re-sends it.
        self._presence_by_room.pop(rm.room, None)
        self._notify_presence(rm)

    def _decref_presence(self, room: str) -> None:
        """Drop one ``Presence`` handle on a room. Listener-only: the room stays
        joined (and snapshots keep routing to remaining handles) until
        ``leave_presence`` is called explicitly — parity with ts-client and
        rust-client. The unsubscribed handle is closed via its own ``closed`` flag."""
        rm = self._presence_by_room.get(room)
        if rm is None:
            return
        rm.handle_count -= 1

    @staticmethod
    def _notify_presence(rm: _PresenceRoom) -> None:
        """Wake any async iterator parked on ``rm.cond``."""

        async def _wake() -> None:
            async with rm.cond:
                rm.cond.notify_all()

        with contextlib.suppress(RuntimeError):
            asyncio.get_running_loop().create_task(_wake())

    def _reject_inflight(self, reason: str) -> None:
        # Only in-flight (sent) entries die on a reconnectable drop; queued
        # entries survive so ``_flush_on_auth`` can resend them on reconnect.
        # Each rejected mutation's overlays are reverted too (a sent-but-unacked
        # mutate never gets a mutateOk/mutateErr, so the rollback must happen
        # here).
        for mp in list(self._pending_mut.values()):
            if mp.sent and not mp.future.done():
                self._pending_mut.pop(mp.id, None)
                self._revert_overlays_for(mp.id)
                mp.future.set_exception(RtDbError(ErrorCode.INTERNAL, reason))
        for sp in list(self._pending_sched.values()):
            if sp.sent and not sp.future.done():
                self._pending_sched.pop(sp.id, None)
                sp.future.set_exception(RtDbError(ErrorCode.INTERNAL, reason))

    def _reject_all_pending(self, reason: str) -> None:
        for mp in list(self._pending_mut.values()):
            if not mp.future.done():
                # Queued mutates also had overlays applied (the apply hook runs
                # in ``mutate`` before the entry is dispatched), so revert them.
                self._revert_overlays_for(mp.id)
                mp.future.set_exception(RtDbError(ErrorCode.INTERNAL, reason))
        self._pending_mut.clear()
        for sp in list(self._pending_sched.values()):
            if not sp.future.done():
                sp.future.set_exception(RtDbError(ErrorCode.INTERNAL, reason))
        self._pending_sched.clear()

    # --- optimistic updates -------------------------------------------
    #
    # When ``optimistic_updates=True``, a ``mutate`` overlays each matching
    # subscription's cached value BEFORE the frame is dispatched (caller-side,
    # so subscribers in other tasks see it before the caller awaits). The next
    # authoritative ``queryUpdate`` reconciles (server-wins, overlay cleared);
    # ``mutateErr`` / a reconnectable drop / ``close()`` roll the overlay back to
    # ``server_last``. Off (the default) ⇒ byte-for-byte the pre-optimistic
    # behavior: none of these hooks mutate ``_Sub`` in a way a caller can observe.

    def _apply_optimistic(self, mut_id: str, txn_dict: dict[str, Any]) -> None:
        """For each live subscription whose projection base is known, project
        ``txn_dict`` onto it; for each non-decline projection, push the overlaid
        value through the sub immediately and record its ``query_id`` under
        ``mut_id`` in ``_overlays`` so a later rollback can find it."""
        now_ms = int(time.time() * 1000)
        steps = txn_dict.get("steps") or []
        touched: set[str] = set()
        for sub in self._subs_by_id.values():
            base = sub.server_last
            if base is None:
                continue
            overlaid, did = _project_optimistic(sub.query, base, steps, now_ms)
            if not did:
                continue
            sub.optimistic_active = True
            sub.value = _reproject(sub, overlaid)
            sub.version += 1
            self._notify(sub)
            touched.add(sub.query_id)
        if touched:
            self._overlays[mut_id] = touched

    def _revert_overlay(self, query_id: str) -> None:
        """Revert one subscription's overlay: if one is active and a projection
        base exists, push the base back through the sub and clear the flag. No-op
        when no overlay is active (e.g. a ``queryUpdate`` already reconciled)."""
        sub = self._subs_by_id.get(query_id)
        if sub is None:
            return
        if sub.optimistic_active and sub.server_last is not None:
            sub.optimistic_active = False
            sub.value = _reproject(sub, sub.server_last)
            sub.version += 1
            self._notify(sub)

    def _revert_overlays_for(self, mut_id: str) -> None:
        """Reverse-index revert: drop ``mut_id``'s entry from ``_overlays`` and
        revert every subscription it had overlaid. Called from ``mutateErr`` and
        the reject paths."""
        qids = self._overlays.pop(mut_id, None)
        if not qids:
            return
        for qid in qids:
            self._revert_overlay(qid)

    def _set_state(self, state: ConnectionState) -> None:
        self._state = state


class Subscription:
    """A live query handle: latest value (``current()``), error (``error()``),
    and an async iterator that yields each new value as it arrives.

    Mirrors the TS/Rust clients' ``Subscription``. Unsubscribe via
    ``.unsubscribe()`` or by closing the async context manager. Multiple
    ``Subscription`` handles on the same query shape share one ``_Sub``; the
    server-side subscription lives until the last handle unsubscribes.
    """

    def __init__(self, client: RtDbClient, sub: _Sub) -> None:
        self._client = client
        self._sub = sub
        self._iter_version = 0

    def current(self) -> Any | None:
        """Most recently delivered value, or ``None`` before the first update."""
        return self._sub.value

    def error(self) -> RtDbError | None:
        """The terminal error if ``subscribeErr`` arrived, else ``None``."""
        return self._sub.error

    def unsubscribe(self) -> None:
        """Drop this handle; sends the server ``unsubscribe`` when the last one leaves."""
        self._client._decref(self._sub.key)

    async def __aenter__(self) -> Subscription:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.unsubscribe()

    def __aiter__(self) -> Subscription:
        # Reset so the iterator yields the current value (if any) first, then
        # each subsequent update.
        self._iter_version = 0
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


class Presence:
    """A presence-room handle (ENH-015): latest member list (``current()``),
    terminal error (``error()``), and an async iterator that yields each fresh
    snapshot. Mirrors :class:`Subscription`'s shape.

    ``unsubscribe()`` only detaches this listener — the room stays joined and
    subsequent snapshots keep routing to remaining handles. Call
    :meth:`RtDbClient.leave_presence` to actually leave the room (parity with
    the ts-client and rust-client). Closing the async context manager
    detaches the listener.
    """

    def __init__(self, client: RtDbClient, room: _PresenceRoom) -> None:
        self._client = client
        self._room = room
        self._iter_version = 0
        self._closed = False

    def current(self) -> list[PresenceMember] | None:
        """Latest member list, or ``None`` before the first ``presenceSnapshot``."""
        return self._room.members

    def error(self) -> RtDbError | None:
        """The terminal error if ``presenceErr`` arrived, else ``None``."""
        return self._room.error

    def unsubscribe(self) -> None:
        """Detach this listener; the room stays joined until ``leave_presence``."""
        if self._closed:
            return
        self._closed = True
        self._client._decref_presence(self._room.room)

    async def __aenter__(self) -> Presence:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.unsubscribe()

    def __aiter__(self) -> Presence:
        self._iter_version = 0
        return self

    async def __anext__(self) -> list[PresenceMember]:
        room = self._room
        if self._closed:
            raise StopAsyncIteration
        async with room.cond:
            while (
                not self._closed
                and room.version <= self._iter_version
                and room.error is None
                and not room.closed
            ):
                await room.cond.wait()
        if self._closed or room.closed:
            raise StopAsyncIteration
        if room.error is not None:
            raise room.error
        self._iter_version = room.version
        # current() is non-None here because version advanced past 0.
        return room.members or []


def _tag(msg: Any) -> str:
    return getattr(msg, "type", "")


def _presence_join_frame(room: str, state: Any | None) -> str:
    """Serialize a ``{type:"presence", room, state?}`` join frame (``state``
    omitted when ``None`` — the wire model's serializer drops it)."""
    return _ClientPresence(room=room, state=state).model_dump_json(by_alias=True)


def _reproject(sub: _Sub, overlaid: Any) -> Any:
    """Re-validate a raw overlaid value through the sub's ``model`` / ``terminal``
    so the overlay matches what ``_on_query_update`` would produce — typed model
    instances for a custom model, fresh dict copies for the default ``model=dict``
    (so a caller mutating ``sub.current()`` cannot corrupt the projection base).
    The try/except falls back to the raw value if a custom model rejects the
    overlaid doc (shouldn't happen, since the overlay derives from the same base
    the server validated). The broad catch is documented here and logged at
    exception level so a real pydantic-schema mismatch surfaces in logs rather
    than degrading silently to the raw value (QA-005)."""
    try:
        return parse_result(sub.model, sub.terminal, overlaid)
    except Exception:
        _logger.exception(
            "ws _reproject fallback: parse_result rejected an overlaid doc "
            "(terminal=%s); returning raw value",
            sub.terminal,
        )
        return overlaid


async def _safe_close(ws: Connection, code: int = 1000) -> None:
    with contextlib.suppress(Exception):
        await ws.close(code, "")


def _auth_frame(token: str, db: str) -> str:
    from .wire import _ClientAuth

    return _ClientAuth(token=token, db=db).model_dump_json(by_alias=True)


def _ping_frame() -> str:
    from .wire import _ClientPing

    return _ClientPing().model_dump_json(by_alias=True)


def _build_sched_frame(kind: str, sid: str, fields: dict[str, Any]) -> str:
    """Serialize a schedule-op frame for correlation id ``sid`` (``sch-{n}``)."""
    if kind == "schedule":
        return _ClientSchedule(
            schedule_id=sid,
            when=fields["when"],
            txn=fields["txn"].model_dump(by_alias=True, mode="json"),
        ).model_dump_json(by_alias=True)
    if kind == "cancel":
        return _ClientCancelSchedule(schedule_id=sid, id=fields["id"]).model_dump_json(
            by_alias=True
        )
    if kind == "pause":
        return _ClientPauseSchedule(schedule_id=sid, id=fields["id"]).model_dump_json(by_alias=True)
    if kind == "resume":
        return _ClientResumeSchedule(schedule_id=sid, id=fields["id"]).model_dump_json(
            by_alias=True
        )
    return _ClientListSchedules(schedule_id=sid).model_dump_json(by_alias=True)


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
        with contextlib.suppress(self._closed):
            await self._raw.close(code, reason)


def _ws_close_code(exc: BaseException) -> int:
    return int(getattr(exc, "code", 1000) or 1000)
