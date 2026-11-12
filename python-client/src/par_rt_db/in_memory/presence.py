"""Presence-engine mixin for the in-memory harness: room join/update/leave,
the presence-ttl expiry sweep, and per-client room bookkeeping. Extracted
from ``store.py`` (QA-002); methods move verbatim onto ``_InMemoryStoreCore``
via mixin assembly in ``__init__.py``."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ..wire import PresenceMember
from .store import PresenceTestHandle as PresenceTestHandle

if TYPE_CHECKING:
    from .store import _InMemoryStoreCore as _Core
else:
    _Core = object


class _PresenceEngine(_Core):
    """_PresenceEngine: methods extracted verbatim from ``_InMemoryStoreCore``."""

    def presence(
        self,
        room: str,
        state: Any | None,
        on_update: Callable[[list[PresenceMember]], None],
    ) -> PresenceTestHandle:
        """Join presence room ``room`` with optional initial ``state``; fire
        ``on_update`` with the current member list on join and again on every
        local mutation (or a peer's join/update/leave on a shared
        :class:`PresenceRooms`).

        The returned :class:`PresenceTestHandle` detaches the listener but does
        NOT leave the room — call :meth:`leave_presence` for that (parity with
        ts-client/rust-client and the reactive client)."""
        self._joined_rooms.add(room)
        member = PresenceMember(
            connection_id=self._connection_id,
            user=self._presence_user.model_copy(),
            state=None if state is None else state,
        )
        # Join first, then subscribe — the initial snapshot (fired synchronously
        # inside subscribe) already includes this connection.
        self._presence_rooms.join(room, member)
        handle = self._presence_rooms.subscribe(room, on_update)
        self._presence_unsubs.setdefault(room, []).append(handle)
        return handle

    def update_presence(self, room: str, state: Any, ttl_ms: int | None = None) -> None:
        """Broadcast updated ``state`` for this connection in ``room``. No-op if
        this client has not joined ``room`` (mirrors the live server ignoring a
        non-member update).

        When ``ttl_ms`` is an ``int > 0``, the harness schedules an expiry that
        nulls this member's ``state`` at ``now + ttl_ms`` (the member stays
        listed); ``None`` (or ``<= 0``) clears any pending expiry. Call
        :meth:`expire_presence` to run the sweep (note: :meth:`tick` only reaps
        document TTL, not presence ttl — parity with ts/rust). The client's
        injected clock (``InMemoryRtDbClientOptions.now``) supplies ``now``, so
        tests that inject a controllable clock get deterministic expiry
        (ENH-015 follow-up; parity with ts/rust)."""
        if room not in self._joined_rooms:
            return
        self._presence_rooms.update(room, self._connection_id, state, ttl_ms, self._now())

    def expire_presence(self, now: int | None = None) -> bool:
        """Run a presence-ttl expiry sweep: clears expired members' ``state`` to
        ``None`` (the member stays listed) and fans out each touched room once.
        Returns ``True`` if anything expired. Mirrors the live server's
        per-connection ttl clearing (``server::presence::expire_once``) and the
        ts/rust harness ``expire``. Use this in tests that don't otherwise drive
        the clock via :meth:`tick`; pass an explicit ``now`` for determinism,
        else the client's injected clock is used (ENH-015 follow-up)."""
        return self._presence_rooms.expire(now if now is not None else self._now())

    def leave_presence(self, room: str) -> None:
        """Leave ``room``: drop every local subscriber this client registered for
        it, remove this connection from the member list, and fan out a fresh
        snapshot to any remaining subscribers. No-op if not joined."""
        if room not in self._joined_rooms:
            return
        self._joined_rooms.discard(room)
        # Drop every local subscriber this client registered for this room.
        handles = self._presence_unsubs.pop(room, [])
        for h in handles:
            h.unsubscribe()
        self._presence_rooms.leave(room, self._connection_id)
