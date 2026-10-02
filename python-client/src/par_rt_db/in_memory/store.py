"""Store core for the in-memory harness (mirrors
``rust-client/src/in_memory/mod.rs``): stored rows/jobs/blobs, presence, the
client core (state, schema push, mutate/transaction entry), and the shared
index-value typing helpers. The per-domain method groups live in sibling
engine mixins — ``writes.py`` (step executor + row ops), ``schedule.py``
(scheduled jobs, ``tick``, TTL reaper), ``workflow.py`` (FM-29 runs), and
``storage.py`` (blob stub) — alongside ``query.py`` (the query engine),
``migrate.py``, and ``validate.py``; the assembled ``InMemoryRtDbClient``
and the former module surface live in ``__init__.py``."""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from pydantic import TypeAdapter

from ..errors import ErrorCode, RtDbError
from ..mutation import (
    Step,
    StepResult,
    Transaction,
    _CancelSchedule,
    _CancelWorkflow,
    _DeleteByQuery,
    _PatchByQuery,
    _Schedule,
    _StartWorkflow,
)
from ..query import Query, _terminal_of, parse_result
from ..schema import (
    IndexDef,
    SchemaDef,
    TableDef,
    _FAny,
    _FArray,
    _FBoolean,
    _FBytes,
    _FId,
    _FInt64,
    _FLiteral,
    _FNull,
    _FNumber,
    _FObject,
    _FOptional,
    _FRecord,
    _FString,
    _FUnion,
    _FVector,
)
from ..wire import (
    AfterMs,
    AuthedUser,
    AwaitSignalSpec,
    Cron,
    Interval,
    PresenceMember,
    RunAt,
    ScheduleInfo,
    ScheduleWhen,
    StepOutcome,
    StepRetry,
    WorkflowInfo,
    WorkflowSpec,
    WorkflowStatus,
)
from .migrate import (
    _detect_destructive_changes,
    _grandfather_trgm,
    _on_delete_ref,
    _validate_on_delete,
)
from .value_expr import _reject_relative_time, _stamp_computed, _validate_computed

#: Maximum number of steps in a single transaction (mirrors the server cap).
MAX_STEPS = 1024
#: Maximum number of steps in one workflow spec (mirrors
#: ``server/src/workflows.rs::MAX_WORKFLOW_STEPS``).
MAX_WORKFLOW_STEPS = 64
#: Maximum rows returned from a single ``take``/``collect`` (mirrors the server cap).
MAX_TAKE = 4096
#: Hard cap on rows a single ``patchByQuery``/``deleteByQuery`` step may touch
#: (mirrors ``server/src/txn.rs::MAX_BY_QUERY_ROWS``). Bounds one serialized
#: turn and prevents a wildcard filter from sweeping a whole table; a larger
#: match set touches exactly this many and reports ``truncated: true``.
MAX_BY_QUERY_ROWS = 1000
#: SEC-104: hard cap on the count of ``patchByQuery``/``deleteByQuery`` steps
#: in one txn (mirrors ``server/src/txn.rs::MAX_BY_QUERY_STEPS_PER_TXN``).
#: Bounds the worst case at 16 x 1000 = 16,000 rows rather than 1024 x 1000
#: (~1M), which would otherwise stall the server's single-writer.
MAX_BY_QUERY_STEPS_PER_TXN = 16
#: SEC-104: hard ceiling on the worst-case total documents a single txn may
#: touch (mirrors ``server/src/txn.rs::MAX_AFFECTED_ROWS_PER_TXN``). Per-id
#: steps count 1 each; each by-query step counts up to its ``limit``.
MAX_AFFECTED_ROWS_PER_TXN = 10_000
#: FM-33: hard cap on the rows one initiating delete step's ``onDelete``
#: cascade may touch (mirrors ``server/src/txn.rs::MAX_CASCADE_ROWS``) —
#: children stamped/deleted/nulled plus each initiator, one shared budget
#: across every row of a ``deleteByQuery`` step. Over-budget raises
#: ``CONFLICT`` so the txn rolls back atomically. Read at call time so tests
#: can pin it via ``monkeypatch.setattr``.
MAX_CASCADE_ROWS = 10_000
#: Approximate cron re-fire interval for the in-memory stub. Real 5-field cron
#: parsing is deferred to the server; the harness only needs crons to re-arm.
CRON_STEP_MS = 60_000
#: Upper bound on an interval job's ``everyMs``: one year in ms (mirrors
#: ``server/src/scheduler.rs::MAX_EVERY_MS``). Bounds the horizon a recurring
#: job can occupy a row for; ``schedule``/the ``schedule`` step reject a
#: non-positive or over-cap value with ``BAD_REQUEST``.
MAX_EVERY_MS = 365 * 24 * 60 * 60 * 1000
#: Serialized-size cap on one signal payload (mirrors
#: ``server/src/workflows.rs::MAX_SIGNAL_PAYLOAD_BYTES``, 64 KiB). Enforced in
#: ``signal_workflow`` before any run lookup — the same first-check position
#: the server's ``deliver_signal`` uses.
MAX_SIGNAL_PAYLOAD_BYTES = 64 * 1024


def _json_structurally_equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _json_structurally_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _json_structurally_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    return type(left) is type(right) and left == right


def worst_case_affected(txn: Transaction) -> int:
    """SEC-104: total documents a txn could touch in the worst case.

    Per-id steps count 1 each; ``schedule``/``cancelSchedule`` and the
    workflow steps (``startWorkflow``/``cancelWorkflow``) count 0
    (control-flow steps touch no documents); each ``patchByQuery``/
    ``deleteByQuery`` step counts up to its ``limit`` (default and cap
    ``MAX_BY_QUERY_ROWS``). Mirrors server ``txn::worst_case_affected``; used
    by ``_execute_transaction``'s ``MAX_AFFECTED_ROWS_PER_TXN`` budget check.
    """
    total = 0
    for step in txn.steps:
        if isinstance(step, (_PatchByQuery, _DeleteByQuery)):
            limit_opt = step.limit
            total += MAX_BY_QUERY_ROWS if limit_opt is None else min(limit_opt, MAX_BY_QUERY_ROWS)
        elif isinstance(step, (_Schedule, _CancelSchedule, _StartWorkflow, _CancelWorkflow)):
            # Control-flow steps touch no documents (server counts 0) —
            # workflow steps included (txn::worst_case_affected).
            continue
        else:
            total += 1
    return total


def _count_steps(txn: Transaction) -> int:
    """FM-28: recursive step count — a ``schedule`` step counts as itself plus
    every step in its nested txn; a ``startWorkflow`` step (FM-29) as itself
    plus every step of every txn in its spec (an ``awaitSignal`` step has no
    txn — it counts 0 there, mirroring the server's ``map_or(0, count_steps)``).
    Mirrors the server's recursive ruling against ``MAX_STEPS`` (a nested tree
    can't smuggle past the flat cap)."""
    total = 0
    for step in txn.steps:
        total += 1
        if isinstance(step, _Schedule):
            total += _count_steps(step.txn)
        elif isinstance(step, _StartWorkflow):
            total += sum(
                _count_steps(Transaction.model_validate(s.txn))
                for s in step.spec.steps
                if s.txn is not None
            )
    return total


def _validate_workflow_spec(spec: WorkflowSpec) -> None:
    """FM-29: submit-time spec validation — same checks and BAD_REQUEST
    messages as server ``workflows::validate_spec`` (and the ts harness's
    ``validateWorkflowSpec``): 1..=MAX_WORKFLOW_STEPS steps, each step carries
    exactly one of txn/awaitSignal (with the wait's name in 1..=256 chars and
    a positive timeoutMs), retry fields in bounds, and the recursive step
    count summed across every step's txn within ``MAX_STEPS`` (the FM-28
    counter — bounds body size and the nesting bomb)."""
    if not spec.steps:
        raise RtDbError(ErrorCode.BAD_REQUEST, "workflow must have at least one step")
    if len(spec.steps) > MAX_WORKFLOW_STEPS:
        raise RtDbError(ErrorCode.BAD_REQUEST, f"workflow exceeds {MAX_WORKFLOW_STEPS} steps")
    for i, step in enumerate(spec.steps):
        if (step.txn is None) == (step.await_signal is None):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"steps[{i}] must carry exactly one of txn or awaitSignal",
            )
        if step.await_signal is not None:
            sig = step.await_signal
            if not sig.name or len(sig.name) > 256:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"steps[{i}].awaitSignal.name must be 1..=256 chars",
                )
            if sig.timeout_ms == 0:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"steps[{i}].awaitSignal.timeoutMs must be > 0",
                )
        if step.retry is not None:
            if step.retry.max_attempts == 0:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"steps[{i}].retry.maxAttempts must be >= 1",
                )
            if (
                step.retry.initial_retry_ms == 0
                or step.retry.max_retry_ms < step.retry.initial_retry_ms
            ):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"steps[{i}].retry requires initialRetryMs > 0"
                    " and maxRetryMs >= initialRetryMs",
                )
    total = sum(
        _count_steps(Transaction.model_validate(s.txn)) for s in spec.steps if s.txn is not None
    )
    if total > MAX_STEPS:
        raise RtDbError(
            ErrorCode.BAD_REQUEST,
            f"workflow recursive step count {total} exceeds MAX_STEPS {MAX_STEPS}",
        )


# Sentinel storage types mirroring ``PgType`` in the Rust harness. Selects the
# comparison domain for index sorts and range bounds (int64 values are stored as
# decimal strings on the wire but must compare numerically).
_TEXT = "text"
_NUMBER = "number"
_BOOLEAN = "boolean"
_INT64 = "int64"
_PgType = str  # one of the four sentinels above
# Friendly type name per storage domain, for eq-value error messages.
_EXPECTED_INDEX_VALUE: dict[_PgType, str] = {
    _TEXT: "a string",
    _NUMBER: "a number",
    _BOOLEAN: "a boolean",
    _INT64: "an int64 string",
}
# Type adapter for constructing ``StepResult`` values without importing the
# private result-variant classes: ``{"id"}`` -> insert result,
# ``{"id","inserted"}`` -> upsert result, ``{patched|deleted, truncated}`` ->
# by-query results, ``{"scheduleId"}`` / ``{"cancelled"}`` -> schedule-step
# results.
_STEP_RESULT = TypeAdapter(StepResult)


@dataclass
class StoredRow:
    """A stored row: the user doc plus its identity/history, kept separate so the
    system fields (``_id``/``_creationTime``/``_version``) are merged in only at
    read time — exactly as the server stores ``doc`` jsonb alongside ``id``/
    ``created_at``/``version`` columns. FM-33: ``deleted_at`` is the soft-delete
    tombstone column (``None`` = live); like the other system fields it is NEVER
    merged into client-visible docs — soft-deleted rows are filtered everywhere
    instead."""

    id: str
    doc: dict[str, Any]
    version: int
    created_at: int
    deleted_at: int | None = None


def _is_live(row: StoredRow) -> bool:
    """FM-33: a soft-deleted row (``deleted_at`` stamped) is invisible to every
    read terminal and write lookup — the harness mirror of the server's
    ``deleted_at IS NULL`` literal."""
    return row.deleted_at is None


@dataclass
class StoredBlob:
    """A stored file blob with its server-side metadata."""

    bytes: bytes
    content_type: str | None
    created_at: int
    sha256: str


@dataclass
class _ScheduledJob:
    """A stored scheduled job. :meth:`InMemoryRtDb.tick` fires due non-paused
    jobs by applying ``txn`` through the same atomic path as :meth:`mutate`.
    External jobs (2026-09-09 external-claim feature) are never fired by
    ``tick()`` — they sit until an application worker claims them over HTTP."""

    id: str
    kind: str  # "oneshot" | "cron" | "interval"
    txn: Transaction
    due_at: int
    cron: str | None
    every_ms: int | None
    status: str  # "pending" | "paused" | "error"
    created_at: int
    fired_count: int
    last_error: str | None
    tz: str | None = None
    external: bool = False
    #: Cumulative count of recurring-job windows that elapsed before a fire.
    #: Computed exactly for interval jobs; always 0 for cron jobs, since this
    #: harness re-arms cron on a fixed CRON_STEP_MS approximation rather than
    #: real cron occurrence math.
    missed_count: int = 0
    last_missed_at: int | None = None


# The awaitSignal slot's "no delivery" state — a sentinel, not None, because
# a delivered JSON null is None in Python (the server distinguishes them as
# SQL NULL vs jsonb null; ``signal_workflow`` always sets the slot). The
# sentinel must be copy-stable: ``_execute_transaction`` rolls back by
# restoring a ``deepcopy`` of the runs store, and a copied sentinel would
# break the ``is not _NO_SIGNAL`` discriminator (a parked run would consume
# a phantom delivery after any failed txn).
class _NoSignal:
    def __repr__(self) -> str:
        return "_NO_SIGNAL"

    def __deepcopy__(self, memo: dict[int, Any]) -> _NoSignal:
        return self

    def __copy__(self) -> _NoSignal:
        return self


_NO_SIGNAL = _NoSignal()


@dataclass
class _WorkflowRun:
    """A stored workflow run (FM-29) — the in-memory mirror of the server's
    ``workflows`` side-table row. The run snapshots its :class:`WorkflowSpec`
    at insert time, so template edits never drift a live run.
    :meth:`InMemoryRtDb.tick` claims due pending/waiting runs (→ running) and
    advances them through :meth:`_advance_run` (the committer
    ``handle_workflow_advance`` port). The awaitSignal side-table columns
    (``wait_name``/``waited_since``/``signal_payload``) are unset on rows
    never parked, and a claimed ``waiting`` row reads them to discriminate a
    delivery (payload set) from a timed-out wait."""

    id: str
    spec: WorkflowSpec
    status: str  # "pending" | "running" | "waiting" | "success" | "failed" | "cancelled"
    current_step: int
    attempts: int
    sleep_until: int | None
    step_outcomes: list[StepOutcome]
    last_error: str | None
    created_at: int
    updated_at: int
    started_at: int | None
    finished_at: int | None
    # awaitSignal side-table columns (unset on rows never parked).
    wait_name: str | None = None
    waited_since: int | None = None
    signal_payload: Any = _NO_SIGNAL

    @property
    def step_count(self) -> int:
        return len(self.spec.steps)


@dataclass
class UploadResult:
    """Result of :meth:`InMemoryRtDb.upload` — server-computed identity, content
    hash, size, and stored ``contentType`` (``None`` when the upload carried none)."""

    id: str
    sha256: str
    size: int
    content_type: str | None


@dataclass
class FileMetadata:
    """File metadata returned by :meth:`InMemoryRtDb.get_file_metadata`. The
    ``sha256`` is the empty string — only the upload result carries the real
    digest (mirrors the live HTTP client)."""

    id: str
    sha256: str
    size: int
    content_type: str | None
    creation_time: int


def _wall_now() -> int:
    """Wall-clock epoch ms; the default clock for :meth:`PresenceRooms.update`
    / :meth:`PresenceRooms.expire` when the caller does not pass an explicit
    ``now`` (tests should pass ``now`` for determinism, mirroring ttl/``tick``)."""
    return int(time.time() * 1000)


class PresenceRooms:
    """Shared in-memory presence backing for tests.

    Mirrors :class:`PresenceRooms` in ts-client/rust-client: a
    ``room → connectionId → member`` map with a per-room subscriber list. Two
    :class:`InMemoryRtDbClient` instances sharing one ``PresenceRooms`` see each
    other's joins/updates/leaves fan out. A client with no ``presence_rooms``
    option gets a private instance and only ever sees itself in its rooms.

    Subscribers fire inline on the mutating caller's thread; never recursively
    mutate the same backing from inside a callback.
    """

    def __init__(self) -> None:
        # room -> list of (connectionId, PresenceMember) preserving join order
        # (matches the TS Map iteration semantics the server's snapshot relies on).
        self._members: dict[str, list[tuple[str, PresenceMember]]] = {}
        # room -> list of (alive flag, callback). The alive flag is a one-element
        # list shared with the handle (same pattern as _Subscription).
        self._subs: dict[str, list[tuple[list[bool], Callable[[list[PresenceMember]], None]]]] = {}
        # room -> connectionId -> expiresAt (ms). Parallel to ``_members`` so the
        # server→client ``PresenceMember`` snapshot shape stays byte-identical —
        # expiry metadata never appears on the wire (parity with ts/rust). ENH-015.
        self._expiry: dict[str, dict[str, int]] = {}

    def snapshot(self, room: str) -> list[PresenceMember]:
        """Current members of ``room`` in join order (empty if no such room)."""
        entries = self._members.get(room)
        if not entries:
            return []
        return [m for _, m in entries]

    def join(self, room: str, member: PresenceMember) -> None:
        """Add or replace ``member`` (keyed by ``connectionId``) and fan out."""
        entries = self._members.setdefault(room, [])
        for i, (cid, _) in enumerate(entries):
            if cid == member.connection_id:
                entries[i] = (member.connection_id, member)
                break
        else:
            entries.append((member.connection_id, member))
        self._fan_out(room)

    def update(
        self,
        room: str,
        connection_id: str,
        state: Any,
        ttl_ms: int | None = None,
        now: int | None = None,
    ) -> None:
        """Update ``connection_id``'s state in ``room`` and fan out. No-op if the
        connection is not in the room (mirrors the live server ignoring a
        non-member update).

        When ``ttl_ms`` is an ``int > 0``, schedules an expiry sweep that nulls
        this member's ``state`` at ``now + ttl_ms`` (the member stays listed);
        ``None`` clears any pending expiry, mirroring the live server's "ttlMs
        after the last refresh" semantics. ``<= 0`` is treated as ``None`` (no
        expiry) — a permissive offline approximation; the LIVE SERVER rejects
        ``ttl_ms <= 0`` with BAD_REQUEST (authoritative). ``now`` defaults to
        wall-clock ms."""
        entries = self._members.get(room)
        if entries is None:
            return
        for i, (cid, existing) in enumerate(entries):
            if cid == connection_id:
                entries[i] = (
                    cid,
                    PresenceMember(connection_id=connection_id, user=existing.user, state=state),
                )
                exp = self._expiry.setdefault(room, {})
                if isinstance(ttl_ms, int) and ttl_ms > 0:
                    exp[connection_id] = (now if now is not None else _wall_now()) + ttl_ms
                else:
                    exp.pop(connection_id, None)
                    if not exp:
                        self._expiry.pop(room, None)
                self._fan_out(room)
                return

    def leave(self, room: str, connection_id: str) -> None:
        """Remove ``connection_id`` from ``room`` and fan out. No-op if absent.
        Also clears any pending expiry entry so a re-join with the same
        connectionId does not inherit a stale ttl (ENH-015 follow-up)."""
        entries = self._members.get(room)
        if entries is None:
            return
        before = len(entries)
        entries[:] = [(cid, m) for cid, m in entries if cid != connection_id]
        if len(entries) == before:
            return  # was not a member — no fan-out
        if not entries:
            self._members.pop(room, None)
        exp = self._expiry.get(room)
        if exp is not None:
            exp.pop(connection_id, None)
            if not exp:
                self._expiry.pop(room, None)
        self._fan_out(room)

    def expire(self, now: int | None = None) -> bool:
        """Clear expired members' ``state`` to ``None`` (the member stays listed)
        and fan out each touched room once. Returns ``True`` if anything expired.
        Mirrors the live server's per-connection ttl clearing
        (``server::presence::expire_once``) and the ts/rust harness's ``expire``
        (ENH-015 follow-up). Idempotent: a second sweep with the same ``now`` is a
        no-op (the expiry entries were drained). ``now`` defaults to wall-clock ms."""
        cur = now if now is not None else _wall_now()
        any_expired = False
        touched: list[str] = []
        # Drain the rooms that currently have an expiry map; we can't iterate
        # ``self._expiry`` while mutating it, so snapshot the rooms first.
        for room in list(self._expiry.keys()):
            exp = self._expiry.get(room)
            if exp is None:
                continue
            entries = self._members.get(room)
            if entries is None:
                # Room was dropped (e.g. last member left) — drop its expiry map.
                self._expiry.pop(room, None)
                continue
            due = [cid for cid, at in exp.items() if at <= cur]
            for cid in due:
                exp.pop(cid, None)
            if not exp:
                self._expiry.pop(room, None)
            if not due:
                continue
            room_touched = False
            for i, (cid, existing) in enumerate(entries):
                if cid in due:
                    entries[i] = (
                        cid,
                        PresenceMember(
                            connection_id=existing.connection_id,
                            user=existing.user,
                            state=None,
                        ),
                    )
                    any_expired = True
                    room_touched = True
            if room_touched:
                touched.append(room)
        for room in touched:
            self._fan_out(room)
        return any_expired

    def subscribe(
        self, room: str, cb: Callable[[list[PresenceMember]], None]
    ) -> PresenceTestHandle:
        """Register ``cb`` for ``room`` snapshots and fire it immediately with the
        current snapshot (mirroring the server's first ``presenceSnapshot`` on
        join). The returned :class:`PresenceTestHandle` detaches the callback."""
        alive = [True]
        self._subs.setdefault(room, []).append((alive, cb))
        cb(self.snapshot(room))
        return PresenceTestHandle(alive)

    def _fan_out(self, room: str) -> None:
        """Re-snapshot ``room`` and fire every live callback; compact dead ones."""
        snap = self.snapshot(room)
        listeners = self._subs.get(room)
        if listeners is None:
            return
        fires = [cb for alive, cb in listeners if alive[0]]
        listeners[:] = [(alive, cb) for alive, cb in listeners if alive[0]]
        if not listeners:
            self._subs.pop(room, None)
        for cb in fires:
            cb(list(snap))


class PresenceTestHandle:
    """Unsubscribe handle returned by :meth:`PresenceRooms.subscribe`.

    Mirrors :class:`SubscriptionHandle`: the alive flag is shared with the
    backing so ``unsubscribe`` (or a ``with`` block) detaches without holding a
    reference to the room. Python's GC is not deterministic, so prefer an
    explicit :meth:`unsubscribe` (or ``with``) over relying on ``__del__``.
    """

    def __init__(self, alive: list[bool]) -> None:
        self._alive = alive

    def unsubscribe(self) -> None:
        """Detach the callback; no further fan-outs fire."""
        self._alive[0] = False

    def __enter__(self) -> PresenceTestHandle:
        return self

    def __exit__(self, *exc: object) -> None:
        self._alive[0] = False


@dataclass
class InMemoryRtDbClientOptions:
    """Injectable clock and RNG for deterministic id minting and
    ``_creationTime``. Both optional; defaults are the system clock and a
    constant ``0.5``. Tests that need determinism should inject both.

    ENH-015 presence options (all optional): ``connection_id`` (stable identity
    in presence rooms; auto-generated as ``c{N}`` when unset), ``presence_user``
    (display identity; defaults to a nameless ``{kind:"user"}``), and
    ``presence_rooms`` (shared backing so two clients see each other).
    """

    now: Callable[[], int] | None = None
    random: Callable[[], float] | None = None
    connection_id: str | None = None
    presence_user: AuthedUser | None = None
    presence_rooms: PresenceRooms | None = None


@dataclass
class _Subscription:
    """Inner state of one reactive subscription. ``alive`` is a one-element
    list shared with the handle so the handle can clear it without holding a
    reference to the client. ``last`` holds the canonical form of the last
    delivered value so the callback only re-fires on a real change."""

    query: Query
    table: str
    alive: list[bool]
    callback: Callable[[Any], None]
    last: str | None = None


class SubscriptionHandle:
    """Unsubscribe handle returned by :meth:`InMemoryRtDb.subscribe`.

    Use :meth:`unsubscribe` (or a ``with`` block) to detach the listener so no
    further updates fire. The Python port does not rely on RAII drop semantics
    (unlike the Rust client) because Python's GC is not deterministic.
    """

    def __init__(self, alive: list[bool]) -> None:
        self._alive = alive

    def unsubscribe(self) -> None:
        """Detach the listener; no further updates fire."""
        self._alive[0] = False

    def __enter__(self) -> SubscriptionHandle:
        return self

    def __exit__(self, *exc: object) -> None:
        self._alive[0] = False


def _insert_result(id: str) -> StepResult:
    return _STEP_RESULT.validate_python({"id": id})


def _upsert_result(id: str, inserted: bool) -> StepResult:
    return _STEP_RESULT.validate_python({"id": id, "inserted": inserted})


def _patch_by_query_result(patched: int, truncated: bool) -> StepResult:
    return _STEP_RESULT.validate_python({"patched": patched, "truncated": truncated})


def _delete_by_query_result(deleted: int, truncated: bool) -> StepResult:
    return _STEP_RESULT.validate_python({"deleted": deleted, "truncated": truncated})


def _schedule_result(schedule_id: str) -> StepResult:
    return _STEP_RESULT.validate_python({"scheduleId": schedule_id})


def _cancel_schedule_result(cancelled: bool) -> StepResult:
    return _STEP_RESULT.validate_python({"cancelled": cancelled})


def _start_workflow_result(workflow_id: str) -> StepResult:
    return _STEP_RESULT.validate_python({"workflowId": workflow_id})


def _cancel_workflow_result(cancelled: bool) -> StepResult:
    return _STEP_RESULT.validate_python({"cancelled": cancelled})


def _canonical(value: Any) -> str:
    """Canonical string form for change detection, independent of key order."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _strip_doc_version(doc: dict[str, Any]) -> dict[str, Any]:
    """One result doc minus its volatile ``_version`` — the diff-comparison
    view only; pushed payloads always carry ``_version``."""
    return {k: v for k, v in doc.items() if k != "_version"}


def _diff_canonical(value: Any, q: Query) -> str:
    """The canonical a subscription diffs against for push decisions.

    For an UNPROJECTED query this is the plain :func:`_canonical` —
    byte-identical push semantics to pre-projection behavior. For a PROJECTED
    query (``fields`` set) the volatile ``_version`` is stripped from every
    doc before serializing: ``_version`` bumps on every write, so an
    unstripped canonical would push on any member write even when no
    projected field changed — defeating the payload-width point of a
    projected subscription. Pushed payloads still carry ``_version``; only
    the change-detection comparison ignores it (a subscriber that must see
    every ``_version`` bump should use an unprojected subscription).
    Mirrors the server's ``query::diff_canonical`` over the typed result
    variants: the doc-bearing terminals (``get``/``first``/``unique`` docs,
    ``collect`` + the search family's doc lists, ``paginate`` pages) strip;
    the doc-less terminals (``count``/``distinct``/``aggregate``) pass through
    untouched."""
    if q.fields is None:
        return _canonical(value)
    terminal = _terminal_of(q)
    if terminal in ("get", "first", "unique"):
        return _canonical(None if value is None else _strip_doc_version(value))
    if terminal == "paginate":
        page = dict(value)
        page["docs"] = [_strip_doc_version(d) for d in value["docs"]]
        return _canonical(page)
    if terminal == "collect":
        # collect + the search family (which `_terminal_of` classifies as collect).
        return _canonical([_strip_doc_version(d) for d in value])
    return _canonical(value)


def _merge_doc(row: StoredRow) -> dict[str, Any]:
    """Merge a stored row with its system fields (``_id``/``_creationTime``/
    ``_version``), layered over the user doc at read time."""
    out = dict(row.doc)
    out["_id"] = row.id
    out["_creationTime"] = row.created_at
    out["_version"] = row.version
    return out


def is_hex_id(value: Any) -> bool:
    """``True`` iff ``value`` is a 32-char lowercase hex string (an ``_id``)."""
    if not isinstance(value, str) or len(value) != 32:
        return False
    return all(c in "0123456789abcdef" for c in value)


def is_int64_string(value: Any) -> bool:
    """``True`` iff ``value`` is a syntactically-valid integer string within
    ``i64`` range (the wire form of an ``int64`` field)."""
    if not isinstance(value, str):
        return False
    digits = value[1:] if value.startswith("-") else value
    if not digits or not all("0" <= c <= "9" for c in digits):
        return False
    try:
        int(value)
        return True
    except ValueError:
        return False


def is_base64_string(value: Any) -> bool:
    """``True`` iff ``value`` is base64-shaped: length a multiple of 4, body in
    ``[A-Za-z0-9+/]``, at most two trailing ``=``."""
    if not isinstance(value, str) or len(value) % 4 != 0:
        return False
    eq_count = len(value) - len(value.rstrip("="))
    if eq_count > 2:
        return False
    body = value[:-eq_count] if eq_count else value
    return all((c.isascii() and c.isalnum()) or c in "+/" for c in body)


def validate_value(ty: Any, value: Any) -> bool:
    """Recursive value validator — a port of server ``schema::validate_value``.
    Switches on the ``FieldType`` variant via ``match`` (pydantic union members
    are narrowed by class pattern, not by literal-field comparison)."""
    match ty:
        case _FString():
            return isinstance(value, str)
        case _FNumber():
            return isinstance(value, float | int) and not isinstance(value, bool)
        case _FBoolean():
            return isinstance(value, bool)
        case _FNull():
            return value is None
        case _FId():
            return is_hex_id(value)
        case _FLiteral(value=lit):
            return value == lit
        case _FOptional(inner=inner):
            return value is None or validate_value(inner, value)
        case _FUnion(variants=variants):
            return any(validate_value(v, value) for v in variants)
        case _FArray(element=element):
            return isinstance(value, list) and all(validate_value(element, i) for i in value)
        case _FObject(fields=fields):
            if not isinstance(value, dict):
                return False
            for key in value:
                if key not in fields:
                    return False
            for field, field_ty in fields.items():
                if field in value:
                    if not validate_value(field_ty, value[field]):
                        return False
                elif not isinstance(field_ty, _FOptional):
                    return False
            return True
        case _FInt64():
            return is_int64_string(value)
        case _FBytes():
            return is_base64_string(value)
        case _FAny():
            return True
        case _FRecord(value=value_ty):
            return isinstance(value, dict) and all(
                validate_value(value_ty, v) for v in value.values()
            )
        case _FVector(dimensions=dims):
            if not isinstance(value, list) or len(value) != dims:
                return False
            return all(
                isinstance(v, float | int) and not isinstance(v, bool) and math.isfinite(v)
                for v in value
            )
        case _:
            return False


def validate_doc(table: TableDef, doc: dict[str, Any]) -> None:
    """Full-document validator. Reserved (``_``-prefixed) and unknown fields are
    rejected; every declared field is either present-and-valid or absent-and-optional.
    Raises :class:`RtDbError` ``SCHEMA_VIOLATION`` on the first violation."""
    for key in doc:
        if key.startswith("_"):
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, f"field '{key}' is reserved")
        if key not in table.fields:
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, f"unknown field '{key}'")
    for field, field_ty in table.fields.items():
        if field in doc:
            if not validate_value(field_ty, doc[field]):
                raise RtDbError(ErrorCode.SCHEMA_VIOLATION, f"field '{field}' has an invalid value")
        elif field_ty.type != "optional":
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, f"field '{field}' is required")


def _apply_defaults(table: TableDef, doc: dict[str, Any]) -> None:
    """FM-32: stamp the table's push-time-validated ``defaults`` onto a NEW
    document in place — every key the doc omits gets the schema's literal.
    Mirrors server ``txn::apply_defaults``: runs after the ttl-default stamp
    (a ttl ``defaultDurationMs`` on the same field wins) and before the
    owner/principal stamps, and only on the new-document paths (insert,
    replace, upsert-insert); patch / upsert-update / patchByQuery never
    re-apply, so clearing an optional field stays cleared. ``deepcopy`` mirrors
    the server's ``value.clone()`` so a nested array/object default is never
    aliased into a stored doc."""
    for field, value in table.defaults.items():
        if field not in doc:
            doc[field] = deepcopy(value)


def _stamp_updated_at(table_def: TableDef, target: dict[str, Any], now: int) -> dict[str, Any]:
    """FM-36: stamp the table's ``updatedAtField`` with ``now`` (epoch ms),
    overwriting any client-supplied value — the same authority family as the
    ttl ``defaultDurationMs`` stamp. Mirrors server ``txn::stamp_updated_at``
    and runs at the same seams, on every version-bumping write path: insert,
    patch, replace, upsert (both branches), patchByQuery, and cascade setNull
    (stamped onto the CHILD table's def). The value follows the field's wire
    convention: a JSON number on a ``number`` field, a decimal string on an
    ``int64`` field. Returns a NEW dict — the incoming doc/fields belong to
    the caller's step and are never mutated."""
    field = table_def.updated_at_field
    if field is None:
        return target
    value: Any = str(now) if isinstance(table_def.fields.get(field), _FInt64) else now
    return {**target, field: value}


def _stored_int_max(docs: dict[tuple[str, str], StoredRow], table: str, field: str) -> int:
    """Largest stored integer value of ``field`` across ``table``'s rows —
    the in-memory stand-in for the server's ``max((doc->>'{field}')::bigint)``
    physical scan (soft-deleted rows included: tombstones are still rows)."""
    best = 0
    for (t, _id), row in docs.items():
        if t != table:
            continue
        value = row.doc.get(field)
        if not isinstance(value, str):
            continue
        try:
            best = max(best, int(value))
        except ValueError:
            continue
    return best


def _optional_rejects_null(ty: Any) -> bool:
    """``True`` iff ``ty`` is an ``Optional`` whose inner type does not itself
    accept ``None`` (so a null value should be stripped to "key absent")."""
    match ty:
        case _FOptional(inner=inner):
            return not validate_value(inner, None)
        case _:
            return False


def _strip_unset_optionals(table: TableDef, doc: dict[str, Any]) -> dict[str, Any]:
    """Remove keys whose value is ``None`` for an ``Optional`` field whose inner
    type does not itself accept ``None`` — the server's single representation of
    an unset optional."""
    out: dict[str, Any] = {}
    for key, value in doc.items():
        field_ty = table.fields.get(key)
        if value is None and field_ty is not None and _optional_rejects_null(field_ty):
            continue
        out[key] = value
    return out


def apply_patch(
    table: TableDef,
    doc: dict[str, Any],
    fields: dict[str, Any],
    now: int = 0,
) -> dict[str, Any]:
    """Apply a patch's ``fields`` onto ``doc``. A ``None`` onto an ``Optional``
    field whose inner type doesn't itself accept ``None`` deletes the key; the
    merged doc is then re-validated whole. Raises ``SCHEMA_VIOLATION`` on violation.

    ENH-028: a computed key in the patch is dropped, not merged — the stamp
    below re-derives it from the final merged doc, so skipping here also keeps
    a wrong-typed client value from failing ``validate_value`` first — and the
    computed fields are stamped after the merge and before the whole-doc
    validation (server ``apply_patch`` order; ``now`` feeds ``Now()``
    expressions, defaulting to 0 only for direct call-site compatibility —
    every engine path passes the client clock)."""
    # FM-37: the auto-increment field is server-assigned and immutable after
    # insert. A patch may carry it back unchanged (round-trip friendly), but
    # any different value — including a type-shifted form of the same number —
    # is rejected. Every patch-shaped path funnels through here (patch,
    # upsert's update branch, patchByQuery), mirroring server ``apply_patch``.
    auto = table.auto_increment_field
    if auto is not None and auto in fields and doc.get(auto) != fields[auto]:
        raise RtDbError(ErrorCode.BAD_REQUEST, f"autoIncrementField '{auto}' cannot be changed")
    merged = dict(doc)
    for fld, value in fields.items():
        if fld in table.computed:
            continue
        field_ty = table.fields.get(fld)
        if field_ty is None:
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, f"unknown field '{fld}'")
        strip = value is None and _optional_rejects_null(field_ty)
        if strip:
            merged.pop(fld, None)
            continue
        if not validate_value(field_ty, value):
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, f"field '{fld}' has an invalid value")
        merged[fld] = value
    merged = _stamp_computed(table, merged, now)
    validate_doc(table, merged)
    return merged


@dataclass
class _IndexedType:
    """Indexed-column storage type plus whether the source field was wrapped in
    ``Optional`` (so callers can let null sort)."""

    pg: _PgType
    nullable: bool


def _index_column_type(ty: Any) -> _IndexedType:
    """Indexable column type — a port of server ``schema::indexed_column_type``.
    Returns ``SCHEMA_VIOLATION`` for non-indexable types."""
    match ty:
        case _FString() | _FId():
            return _IndexedType(_TEXT, False)
        case _FNumber():
            return _IndexedType(_NUMBER, False)
        case _FBoolean():
            return _IndexedType(_BOOLEAN, False)
        case _FInt64():
            return _IndexedType(_INT64, False)
        case _FLiteral(value=v):
            if isinstance(v, str):
                return _IndexedType(_TEXT, False)
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, "field type 'literal' is not indexable")
        case _FUnion(variants=variants):
            if all(isinstance(v, _FLiteral) and isinstance(v.value, str) for v in variants):
                return _IndexedType(_TEXT, False)
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, "field type 'union' is not indexable")
        case _FOptional(inner=inner):
            inner_ty = _index_column_type(inner)
            return _IndexedType(inner_ty.pg, True)
        case _:
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, f"field type '{ty.type}' is not indexable")


def _validate_schema(schema: SchemaDef) -> None:
    """Push-time schema validation — the TTL, ``updatedAtField``, and
    index-field rules of server ``schema.rs::validate`` (``validate_indexes`` +
    ``validate_ttl`` + ``validate_updated_at``), mirroring the rust harness's
    ``SchemaDef::validate`` and the TS ``validateSchema``: index fields must be
    declared and indexable, search indexes must cover text fields, a TTL must
    name a numeric field carrying a single-field, non-unique, non-partial
    btree index, and an ``updatedAtField`` must be a declared number/int64
    field distinct from ``ttl.field``. Deliberately a subset — identifier
    formats, owner/collaborator fields, defaults, and ``onDelete`` shapes stay
    server-side (the last has its own ``_validate_on_delete`` pass)."""
    for table_name, table in schema.tables.items():
        # `authorize` predicates reject `olderThan` at push (server
        # `validate_structure`: the op is by-query-only and authorize runs on
        # reads/writes with no execution-clock cutoff).
        if table.authorize is not None:
            _reject_relative_time(table.authorize)
        for index in table.indexes:
            if not index.fields:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"index '{index.name}' on table '{table_name}' has no fields",
                )
            # A partial-index predicate is baked into DDL as a literal — an
            # execution-time-relative `olderThan` cutoff has no static meaning
            # there (server `compile_filter_literal` rejects it at push).
            if index.where is not None:
                _reject_relative_time(
                    index.where,
                    "olderThan filter is not allowed in a partial-index predicate",
                    code=ErrorCode.BAD_REQUEST,
                )
            # A vector index's ``fields[0]`` is a Vector column, which is not
            # btree-indexable — the server validates vector specs in their own
            # branch and skips the per-field loop below.
            if index.vector is not None:
                continue
            for field_name in index.fields:
                field_type = table.fields.get(field_name)
                if field_type is None:
                    raise RtDbError(
                        ErrorCode.SCHEMA_VIOLATION,
                        f"index '{index.name}' on table '{table_name}' references unknown"
                        f" field '{field_name}'",
                    )
                pg = _index_column_type(field_type).pg
                if index.search and pg != _TEXT:
                    raise RtDbError(
                        ErrorCode.SCHEMA_VIOLATION,
                        f"search index '{index.name}' on table '{table_name}' has non-text"
                        f" field '{field_name}'",
                    )
        ttl = table.ttl
        if ttl is not None:
            field_type = table.fields.get(ttl.field)
            if field_type is None:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"ttl.field '{ttl.field}' is not a declared field",
                )
            if not isinstance(field_type, _FNumber | _FInt64):
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"ttl.field '{ttl.field}' must be a number or bigint field",
                )
            has_ttl_index = any(
                not idx.search
                and idx.vector is None
                and not idx.unique
                and idx.where is None
                and len(idx.fields) == 1
                and idx.fields[0] == ttl.field
                for idx in table.indexes
            )
            if not has_ttl_index:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"ttl.field '{ttl.field}' requires a single-field, non-unique,"
                    " non-partial btree index on it",
                )
            if ttl.default_duration_ms is not None and ttl.default_duration_ms <= 0:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    "ttl.defaultDurationMs must be greater than 0",
                )
        # FM-36: `updatedAtField` names a declared number/int64 field (the
        # stamp is an epoch-ms number — a decimal string on an int64 field)
        # distinct from `ttl.field` (both stamps write unconditionally, so a
        # shared field would silently drop the expiry). No index required:
        # the stamp never queries the field (server `validate_updated_at`;
        # the identifier-format check stays server-side like the other
        # identifier rules).
        updated_at = table.updated_at_field
        if updated_at is not None:
            updated_at_ty = table.fields.get(updated_at)
            if updated_at_ty is None:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"updatedAtField '{updated_at}' is not a declared field",
                )
            if not isinstance(updated_at_ty, _FNumber | _FInt64):
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"updatedAtField '{updated_at}' must be a number or bigint field",
                )
            if ttl is not None and ttl.field == updated_at:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"updatedAtField '{updated_at}' must differ from ttl.field (both stamps"
                    " write unconditionally; a shared field would drop the expiry)",
                )
        # FM-37: `autoIncrementField` names a declared `int64` field exactly
        # (the counter produces int64; a `number` would lose precision, an
        # `optional` would admit a missing counter) and must differ from
        # `ttl.field` and `updatedAtField` (both stamp unconditionally on
        # writes the counter must survive verbatim). A `defaults` entry on the
        # field is allowed but always loses to the stamp (server
        # `validate_auto_increment`; the identifier-format check stays
        # server-side like the other identifier rules).
        auto = table.auto_increment_field
        if auto is not None:
            auto_ty = table.fields.get(auto)
            if auto_ty is None:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"autoIncrementField '{auto}' is not a declared field",
                )
            if not isinstance(auto_ty, _FInt64):
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"autoIncrementField '{auto}' must be an int64 field",
                )
            if ttl is not None and ttl.field == auto:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"autoIncrementField '{auto}' must differ from ttl.field (the ttl reaper"
                    " would delete counter rows)",
                )
            if table.updated_at_field == auto:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"autoIncrementField '{auto}' must differ from updatedAtField (the"
                    " timestamp would overwrite the counter on every write)",
                )


def _pg_for_field(table_def: TableDef, field: str) -> _PgType:
    """Storage type for an index sort column, defensive ``_TEXT`` fallback."""
    field_ty = table_def.fields.get(field)
    if field_ty is None:
        return _TEXT
    try:
        return _index_column_type(field_ty).pg
    except RtDbError:
        return _TEXT


def _coerce_index_value(table_def: TableDef, field_name: str, value: Any) -> Any:
    """Type-check an eq/range bind value. Returns the value unchanged on success."""
    field_ty = table_def.fields.get(field_name)
    if field_ty is None:
        raise RtDbError(ErrorCode.INTERNAL, f"index references unknown field '{field_name}'")
    pg = _index_column_type(field_ty).pg
    if pg == _TEXT:
        ok = isinstance(value, str)
    elif pg == _NUMBER:
        ok = isinstance(value, float | int) and not isinstance(value, bool)
    elif pg == _BOOLEAN:
        ok = isinstance(value, bool)
    elif pg == _INT64:
        ok = is_int64_string(value)
    else:
        ok = True
    if not ok:
        raise RtDbError(ErrorCode.BAD_REQUEST, f"eq value must be {_EXPECTED_INDEX_VALUE[pg]}")
    return value


def _compare_index_values(a: Any, b: Any, pg: _PgType) -> int:
    """Null-sorting comparison for one index sort key. Numbers compare
    numerically, strings lexicographically, booleans as ``false < true``; nulls
    sort last (asc) / first (desc, via the caller flipping the result). Returns
    ``-1``/``0``/``1``. ``_INT64`` parses the decimal string to ``int`` so int64
    index values sort/range numerically rather than lexicographically."""
    a_null = a is None
    b_null = b is None
    if a_null and b_null:
        return 0
    if a_null:
        return 1
    if b_null:
        return -1
    if pg == _INT64:
        an = _parse_i64(a)
        bn = _parse_i64(b)
        return (an > bn) - (an < bn)
    if pg == _NUMBER:
        av = _to_float(a)
        bv = _to_float(b)
        return (av > bv) - (av < bv)  # NaN comparisons are False -> 0 (Equal)
    if isinstance(a, str) and isinstance(b, str):
        return (a > b) - (a < b)
    if isinstance(a, bool) and isinstance(b, bool):
        return int(a) - int(b)
    return 0


def _parse_i64(value: Any) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return -(1 << 63)  # mirrors Rust's i64::MIN fallback for unparseable values


def _to_float(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, float | int):
        return float(value)
    return float("nan")


def _require_index(table_def: TableDef, name: str) -> IndexDef:
    for index in table_def.indexes:
        if index.name == name:
            return index
    raise RtDbError(ErrorCode.BAD_REQUEST, f"index '{name}' not found")


def _collect_index_key(fields: list[str], doc: dict[str, Any]) -> tuple[Any, ...] | None:
    """Build the collision key for a unique-index lookup over ``fields`` from a
    stored doc. Returns ``None`` if any key field is null/absent — Postgres
    ``UNIQUE`` treats NULLs as distinct, so such a row is exempt from the
    constraint (mirrors server ``schema.rs`` and the TS/Rust harnesses). The key
    is the declared ``fields`` only — never ``id`` or ``created_at``."""
    key: list[Any] = []
    for f in fields:
        v = doc.get(f)
        if v is None:
            return None
        key.append(v)
    return tuple(key)


def _base36(n: int) -> str:
    """Lowercase base-36 encoding, matching JS ``Number.prototype.toString(36)``."""
    if n == 0:
        return "0"
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    out: list[str] = []
    while n > 0:
        out.append(chars[n % 36])
        n //= 36
    return "".join(reversed(out))


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _schedule_info(job: _ScheduledJob) -> ScheduleInfo:
    return ScheduleInfo.model_validate(
        {
            "id": job.id,
            "kind": job.kind,
            "dueAt": job.due_at,
            "cron": job.cron,
            "everyMs": job.every_ms,
            "tz": job.tz,
            "status": job.status,
            "lastError": job.last_error,
            "createdAt": job.created_at,
            "firedCount": job.fired_count,
            "external": job.external,
            "missedCount": job.missed_count,
            "lastMissedAt": job.last_missed_at,
        }
    )


#: The retry policy applied when a step omits ``retry`` — the server's Default
#: (3 attempts, 1s initial backoff doubling to a 60s cap).
_DEFAULT_STEP_RETRY = StepRetry(max_attempts=3)

#: The timeout gate for an awaitSignal step that omits ``timeoutMs`` — the
#: server's ``i64::MAX`` "never due" sentinel. Only a delivery or a cancel
#: wakes such a run; the claim pass never finds it due.
_NEVER_DUE = 2**63 - 1


def _workflow_info(run: _WorkflowRun) -> WorkflowInfo:
    # The wait columns project ONLY while ``waiting`` (server: a terminal or
    # never-parked row never leaks stale wait fields).
    waiting = run.status == "waiting"
    return WorkflowInfo.model_validate(
        {
            "id": run.id,
            "name": run.spec.name,
            "status": run.status,
            "currentStep": run.current_step,
            "stepCount": run.step_count,
            "attempts": run.attempts,
            "sleepUntil": run.sleep_until,
            "lastError": run.last_error,
            "waitingFor": run.wait_name if waiting else None,
            "waitedSince": run.waited_since if waiting else None,
            "createdAt": run.created_at,
            "updatedAt": run.updated_at,
            "startedAt": run.started_at,
            "finishedAt": run.finished_at,
        }
    )


class _InMemoryStoreCore:
    """Client core: state, transactions, schedules/workflows, storage, and
    presence. The query engine methods (``run_query`` and the per-terminal
    executors) come from ``_QueryEngine`` and the migration engine from
    ``_MigrateEngine``; ``InMemoryRtDbClient`` (in ``__init__.py``) assembles
    the three."""

    if TYPE_CHECKING:
        # Provided by the sibling engine mixins at assembly
        # (par_rt_db/in_memory/__init__.py): the query engine's dispatcher,
        # writes.py's step executor, and writes.py's cascade walker.
        def run_query(self, q: Query) -> Any: ...

        def _execute_step(self, step: Step) -> tuple[StepResult, set[str]]: ...

        def _delete_row_cascade(
            self,
            table_name: str,
            sid: str,
            visited: set[tuple[str, str]],
            cascade_rows: list[int],
            force_hard: bool,
            touched: set[str],
        ) -> None: ...

    def __init__(self, options: InMemoryRtDbClientOptions | None = None) -> None:
        opts = options or InMemoryRtDbClientOptions()
        self._now: Callable[[], int] = opts.now or (lambda: int(time.time() * 1000))
        self._random: Callable[[], float] = opts.random or (lambda: 0.5)
        self._schema: SchemaDef | None = None
        # Per-table schema defs, keyed by table name. Separate from `_schema` so
        # the hot paths (validate-on-write, table lookups) don't re-walk it.
        self._tables: dict[str, TableDef] = {}
        # Document store keyed by (table_name, id).
        self._docs: dict[tuple[str, str], StoredRow] = {}
        # Counter for storage-upload id minting and the `_new_id` uniqueness
        # suffix (the TS harness shares one `idCounter` for both; connection_id
        # minting has its own `_conn_counter` here).
        self._id_counter: int = 0
        # FM-37: per-table auto-increment counters — the LAST value handed out
        # (0 = untouched), the stand-in for the server's per-table Postgres
        # sequence. Deliberately NOT part of the txn rollback snapshot: the
        # server's `nextval` is non-transactional, so a rolled-back txn still
        # consumes its number (monotonic, not gap-free) — same here.
        self._auto_counters: dict[str, int] = {}
        # mut_id -> cached results (idempotency short-circuit).
        self._idempotency: dict[str, list[StepResult]] = {}
        # Per-database read-only freeze (server ``PATCH /admin/db/{db}/readonly``).
        # While set, ``mutate`` fails with ``READ_ONLY`` after the
        # idempotency-replay lookup; queries are unaffected.
        self._read_only: bool = False
        # Scheduled jobs (one-shot + cron).
        self._schedules: list[_ScheduledJob] = []
        # Workflow runs (FM-29), insertion-ordered.
        self._workflows: list[_WorkflowRun] = []
        # Reactive subscriptions.
        self._subscribers: list[_Subscription] = []
        # Storage stub: per-id blobs.
        self._storage: dict[str, StoredBlob] = {}
        # ENH-015 presence backing + per-client identity. ``_conn_counter`` is a
        # separate counter so connection_id minting does not shift storage ids.
        self._presence_rooms: PresenceRooms = opts.presence_rooms or PresenceRooms()
        self._presence_user: AuthedUser = opts.presence_user or AuthedUser(kind="user")
        self._conn_counter: int = 0
        self._connection_id: str = opts.connection_id or self._mint_connection_id()
        # Rooms this client has joined (for update/leave bookkeeping).
        self._joined_rooms: set[str] = set()
        # Unsubscribe handles for this client's registered presence callbacks,
        # keyed by room (so leave_presence can drop every local subscriber).
        self._presence_unsubs: dict[str, list[PresenceTestHandle]] = {}

    def _mint_connection_id(self) -> str:
        """Auto-generated identity when ``connection_id`` is not injected
        (mirrors the TS harness's ``c{N}`` counter)."""
        self._conn_counter += 1
        return f"c{self._conn_counter}"

    def set_read_only(self, read_only: bool) -> None:
        """Toggle the per-database read-only freeze (server
        ``PATCH /admin/db/{db}/readonly``). While set, ``mutate`` fails with
        ``READ_ONLY`` after the idempotency-replay lookup; queries are
        unaffected."""
        self._read_only = read_only

    def push_schema(self, schema: SchemaDef) -> None:
        """Install ``schema`` as this client's sole in-memory database schema,
        merging additively on subsequent pushes: existing docs and idempotency
        entries are preserved, and ``_tables`` is repopulated from the new schema
        (folding in new fields/indexes/tables without touching rows). Every
        push validates TTL and index-field rules (``_validate_schema``, the
        server's ``schema.validate()`` order). Destructive changes — a
        removed/changed table, field, or index — raise :class:`RtDbError`
        ``BAD_REQUEST`` with the same messages as the live server's
        ``ddl.rs::detect_destructive_changes``."""
        _validate_schema(schema)
        # FM-30: mirrors server `ddl::push_schema` — grandfather `trgm` on a
        # search index that already existed before this push declared it,
        # before the (deliberately trgm-blind) destructive-change check.
        _grandfather_trgm(self._schema, schema)
        if self._schema is not None:
            _detect_destructive_changes(self._schema, schema)
        # FM-33: the server validates `onDelete` placement/shape in
        # `SchemaDef::validate` on every push; mirror both passes here.
        _validate_on_delete(schema)
        # ENH-028: computed-field validation (keys declared and non-stamped,
        # references declared and non-computed, marker-free `case.whens`,
        # static-kind fit, authorize interplay) — `BAD_REQUEST` on the same
        # rules as the server's push path.
        _validate_computed(schema)
        # FM-37: mirror `ddl::apply_sequence` — when a table's declaration is
        # newly added or changed, reposition its counter so the next stamp is
        # strictly past every stored value of the new field AND strictly past
        # anything already consumed (forward-only: max with the current
        # position). An unchanged declaration leaves the counter untouched (a
        # re-push must not disturb it); a REMOVED declaration keeps the
        # counter too (the server's sequence persists until the table drops).
        for name, new_def in schema.tables.items():
            field = new_def.auto_increment_field
            if field is None:
                continue
            old = self._tables.get(name)
            if old is not None and old.auto_increment_field == field:
                continue
            stored_max = _stored_int_max(self._docs, name, field)
            self._auto_counters[name] = max(self._auto_counters.get(name, 0), stored_max)
        self._schema = schema
        for name, def_ in schema.tables.items():
            self._tables[name] = def_

    def to_schema_json(self) -> SchemaDef | None:
        """Snapshot of the currently-installed schema (or ``None`` before
        :meth:`push_schema`)."""
        return self._schema

    def get(self, table: str, id: str) -> dict[str, Any] | None:
        """Minimal point read — the merged doc (system fields included) for
        ``(table, id)``, or ``None`` if absent. FM-33: a soft-deleted row is
        absent (the server's ``compile_point_read`` ``deleted_at IS NULL``)."""
        row = self._docs.get((table, id))
        return None if row is None or not _is_live(row) else _merge_doc(row)

    def collect_all(self, table: str) -> list[dict[str, Any]]:
        """Test/debug helper — every merged doc in ``table``, in unspecified
        order. Not part of the query DSL. FM-33: soft-deleted rows are skipped."""
        return [
            _merge_doc(row) for (t, _), row in self._docs.items() if t == table and _is_live(row)
        ]

    def run(self, q: Query, model: type = dict) -> Any:
        """Typed wrapper around :meth:`run_query` that deserializes the result
        via :func:`par_rt_db.query.parse_result`. Pick ``model`` to match the
        terminal: ``list`` for ``take``/``collect`` (default ``dict`` per-doc),
        ``dict``/a Pydantic model for ``get``/``first``/``unique``, ``int`` for
        ``count``, ``Paginated`` shape for ``paginate``."""
        value = self.run_query(q)
        return parse_result(model, _terminal_of(q), value)

    def mutate(self, txn: Transaction, mut_id: str | None = None) -> list[StepResult]:
        """Execute a transaction and return one :class:`StepResult` per step, in
        order. A ``mut_id`` seen before short-circuits with the cached results
        (idempotency). On any step error the whole txn rolls back atomically and
        reactive subscriptions see nothing."""
        if mut_id is not None:
            cached = self._idempotency.get(mut_id)
            if cached is not None:
                return cached
        # Per-database read-only freeze (corpus ``dbReadOnly`` cases): mirrors
        # the server committer's Mutate arm — the gate sits after the
        # idempotency-replay lookup.
        if self._read_only:
            raise RtDbError(
                ErrorCode.READ_ONLY,
                "database is frozen read-only; an operator can unfreeze it via "
                "PATCH /admin/db/{db}/readonly",
            )
        results = self._execute_transaction(txn)
        if mut_id is not None:
            self._idempotency[mut_id] = list(results)
        return results

    def _execute_transaction(self, txn: Transaction) -> list[StepResult]:
        # FM-28: count recursively — a schedule step contributes its nested
        # txn's steps too, mirroring the server's recursive MAX_STEPS ruling.
        if _count_steps(txn) > MAX_STEPS:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"transaction exceeds maximum of {MAX_STEPS} steps",
            )
        # SEC-104: bound the worst-case row count before any step applies so an
        # over-budget txn rolls back nothing. Mirrors server ``execute_txn``.
        by_query_steps = sum(1 for s in txn.steps if isinstance(s, (_PatchByQuery, _DeleteByQuery)))
        if by_query_steps > MAX_BY_QUERY_STEPS_PER_TXN:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"transaction has {by_query_steps} by-query steps, exceeding the limit "
                f"of {MAX_BY_QUERY_STEPS_PER_TXN}",
            )
        worst = worst_case_affected(txn)
        if worst > MAX_AFFECTED_ROWS_PER_TXN:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"transaction could affect up to {worst} documents, exceeding the limit "
                f"of {MAX_AFFECTED_ROWS_PER_TXN}",
            )
        snapshot = deepcopy(self._docs)
        # FM-28: schedule/cancelSchedule steps mutate the pending-jobs store, so
        # it joins the rollback snapshot — a failed later step must not leave a
        # phantom enqueued (or cancelled) job behind, mirroring the server's
        # single sqlx transaction around the scheduled_txns insert.
        # shallow copy; jobs are appended/removed, never edited in place
        schedules_snapshot = list(self._schedules)
        # FM-29: workflow steps mutate the runs store too — a startWorkflow
        # appends and a cancelWorkflow flips status in place, so this snapshot
        # is a deep copy (a rolled-back txn leaves no orphan run and no phantom
        # cancel, mirroring the server's single sqlx transaction). Deep-copied
        # because runs ARE edited in place, unlike scheduled jobs.
        workflows_snapshot = deepcopy(self._workflows)
        results: list[StepResult] = []
        write_set: set[str] = set()
        for step in txn.steps:
            try:
                result, written_tables = self._execute_step(step)
            except RtDbError:
                # Atomicity: any step's error rolls back everything already applied.
                self._docs = snapshot
                self._schedules = schedules_snapshot
                self._workflows = workflows_snapshot
                raise
            results.append(result)
            write_set.update(written_tables)
        self._notify_subs(write_set)
        return results

    def _require_table(self, name: str) -> TableDef:
        def_ = self._tables.get(name)
        if def_ is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"table '{name}' not found")
        return def_

    def _new_id(self) -> str:
        # The counter suffix guarantees uniqueness even under a deterministic
        # ``random=lambda: 0.0`` — two ids minted in the same pinned instant
        # (e.g. two workflow steps firing in one tick) must never collide
        # (mirrors the TS harness's ``newId``).
        ts = self._now() & ((1 << 48) - 1)
        n = self._id_counter % 0x1000000
        self._id_counter += 1
        rand = self._random_hex(13) + f"{n:06x}"
        return f"{ts:012x}7{rand}"

    def _random_hex(self, count: int) -> str:
        digits = "0123456789abcdef"
        return "".join(digits[int(self._random() * 16) & 0xF] for _ in range(count))

    def subscribe(self, query: Query, on_update: Callable[[Any], None]) -> SubscriptionHandle:
        """Reactive subscription — fires ``on_update`` with the initial result
        synchronously, then again whenever a mutation changes the result. The
        returned handle detaches the listener via :meth:`SubscriptionHandle.unsubscribe`
        or a ``with`` block. The callback runs inline on the writing thread; never
        recursively mutate the same client from inside a callback."""
        alive = [True]
        table = query.table
        sub = _Subscription(query=query, table=table, alive=alive, callback=on_update)
        self._subscribers.append(sub)
        # Initial value, delivered synchronously (the server's first queryUpdate).
        # A failing query simply never fires — tests that need to assert on a
        # failing query should call run_query directly.
        try:
            initial = self.run_query(query)
        except RtDbError:
            initial = None
        sub.last = _diff_canonical(initial, query)
        on_update(initial)
        return SubscriptionHandle(alive)

    def _notify_subs(self, write_set: set[str]) -> None:
        # Collect work before firing so callbacks run outside the iteration (a
        # callback that re-enters the client does not break the loop).
        fires: list[tuple[Callable[[Any], None], Any]] = []
        for sub in self._subscribers:
            if not sub.alive[0] or sub.table not in write_set:
                continue
            try:
                nxt = self.run_query(sub.query)
            except RtDbError:
                continue  # suppress: a bad subscriber query must not abort the write
            nxt_canon = _diff_canonical(nxt, sub.query)
            if sub.last is None or sub.last != nxt_canon:
                sub.last = nxt_canon
                fires.append((sub.callback, nxt))
        # Lazily compact dead subscriptions.
        self._subscribers = [s for s in self._subscribers if s.alive[0]]
        for callback, value in fires:
            callback(value)

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

    def _prepare_job(self, when: ScheduleWhen) -> tuple[str, int, str | None, int | None]:
        """(kind, due_at, cron, every_ms) for a ``ScheduleWhen`` — shared by the
        standalone ``schedule`` op and the ``schedule`` txn step. The clock
        comes from the injectable ``now``, never ``time.time()`` directly.
        ``everyMs`` is validated here (positive, at most :data:`MAX_EVERY_MS`)
        before any row is created, mirroring the server's ``resolve_when``."""
        now = self._now()
        match when:
            case Interval(every_ms=every_ms):
                if every_ms <= 0:
                    raise RtDbError(ErrorCode.BAD_REQUEST, "everyMs must be positive")
                if every_ms > MAX_EVERY_MS:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST, f"everyMs must be at most {MAX_EVERY_MS}"
                    )
                return "interval", self._due_at_for(when, now), None, every_ms
            case Cron(expr=expr_str):
                return "cron", self._due_at_for(when, now), expr_str, None
            case _:
                return "oneshot", self._due_at_for(when, now), None, None

    def schedule(self, txn: Transaction, when: ScheduleWhen, *, external: bool = False) -> str:
        """Store ``txn`` scheduled for ``when`` and return its id. Cron
        validation is deferred to the live server; the harness accepts any
        expression. ``everyMs`` (interval) IS validated — positive and at most
        :data:`MAX_EVERY_MS` — mirroring the server's ``resolve_when``. An
        external job is never fired by :meth:`tick` — it sits ``pending`` until
        an application worker claims it (mirrors the server's external-claim
        surface)."""
        new_id = self._new_id()
        kind, due_at, cron, every_ms = self._prepare_job(when)
        self._schedules.append(
            _ScheduledJob(
                id=new_id,
                kind=kind,
                txn=txn,
                due_at=due_at,
                cron=cron,
                every_ms=every_ms,
                status="pending",
                created_at=self._now(),
                fired_count=0,
                last_error=None,
                external=external,
            )
        )
        return new_id

    def cancel_schedule(self, id: str) -> bool:
        """Remove the scheduled job. ``False`` when no such id exists (a no-op,
        not an error) — the server's ``scheduler::cancel`` contract."""
        before = len(self._schedules)
        self._schedules = [j for j in self._schedules if j.id != id]
        return len(self._schedules) != before

    def pause_schedule(self, id: str) -> bool:
        """Flip a pending job to ``paused``. ``False`` when the job is missing
        or not pending (a no-op, not an error)."""
        job = self._find_job(id)
        if job is None or job.status != "pending":
            return False
        job.status = "paused"
        return True

    def resume_schedule(self, id: str) -> bool:
        """Flip a paused job back to ``pending``. ``False`` when the job is
        missing or not paused (a no-op, not an error). An interval job's
        ``due_at`` shifts to ``now + everyMs`` (windows elapsed while paused
        are skipped, never backfilled — mirrors the server's ``set_paused``
        resume arm); one-shots and crons keep their ``due_at`` (the harness
        cannot recompute a cron's next fire)."""
        job = self._find_job(id)
        if job is None or job.status != "paused":
            return False
        job.status = "pending"
        if job.kind == "interval" and job.every_ms is not None:
            job.due_at = self._now() + job.every_ms
        return True

    def list_schedules(self) -> list[ScheduleInfo]:
        """Snapshot of every scheduled job's public view."""
        return [_schedule_info(job) for job in self._schedules]

    def start_workflow(self, spec: WorkflowSpec) -> str:
        """Insert a run from ``spec`` and return its id. The run starts
        ``pending`` at step 0; the first step's ``sleepBeforeMs`` gates its
        initial claim (``tick()`` advances it afterwards)."""
        return self._insert_workflow(spec)

    def cancel_workflow(self, id: str) -> bool:
        """Flip a pending/running/waiting run to ``cancelled``. ``False`` when
        the run is missing or already terminal (a no-op, not an error). The
        wait columns drop with the flip (leave-``waiting`` rule) — a cancelled
        run never matches a later signal delivery."""
        run = self._find_workflow(id)
        if run is None or run.status not in ("pending", "running", "waiting"):
            return False
        now = self._now()
        run.status = "cancelled"
        run.wait_name = None
        run.waited_since = None
        run.signal_payload = _NO_SIGNAL
        run.updated_at = now
        run.finished_at = now
        return True

    def signal_workflow(self, id: str, name: str, payload: Any | None = None) -> bool:
        """Deliver a named signal to a run parked at an ``awaitSignal`` step —
        the in-memory mirror of server ``workflows::deliver_signal``. The slot
        is latest-wins (every delivery while the wait is unconsumed overwrites
        the payload) and the wake lands on the NEXT tick: the run flips to
        ``pending`` due immediately, and the claim pass advances it. An
        omitted payload is delivered as JSON null — the slot is always set on
        a delivery, so the wait is consumed either way.

        Raises the server's typed errors: ``NOT_FOUND`` for an unknown run,
        ``CONFLICT`` when the run is not waiting for a signal, and ``CONFLICT``
        naming both names when it waits on a different one. A payload over
        64 KiB serialized rejects ``BAD_REQUEST``."""
        if payload is not None:
            size = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
            if size > MAX_SIGNAL_PAYLOAD_BYTES:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"signal payload exceeds {MAX_SIGNAL_PAYLOAD_BYTES} bytes",
                )
        run = self._find_workflow(id)
        if run is None:
            raise RtDbError(ErrorCode.NOT_FOUND, "unknown workflow")
        if run.status in ("waiting", "pending") and run.wait_name == name:
            now = self._now()
            run.status = "pending"
            run.sleep_until = now
            run.signal_payload = payload
            run.updated_at = now
            return True
        if run.status == "waiting":
            raise RtDbError(
                ErrorCode.CONFLICT,
                f"workflow waiting on '{run.wait_name}', got '{name}'",
            )
        raise RtDbError(ErrorCode.CONFLICT, "workflow is not waiting for a signal")

    def list_workflows(self, status: WorkflowStatus | None = None) -> list[WorkflowInfo]:
        """Every run's info projection, newest first; ``status`` filters to a
        lifecycle state."""
        runs = [r for r in self._workflows if status is None or r.status == status]
        runs.sort(key=lambda r: r.created_at, reverse=True)
        return [_workflow_info(r) for r in runs]

    def _insert_workflow(self, spec: WorkflowSpec) -> str:
        _validate_workflow_spec(spec)
        now = self._now()
        # The server column is NOT NULL — the insert gate is always
        # ``now + unwrap_or(0)``: sleepBeforeMs absent/0 means due immediately
        # (gate == the insert instant), not "no gate".
        gate = now + (spec.steps[0].sleep_before_ms or 0)
        run = _WorkflowRun(
            id=self._new_id(),
            spec=spec,
            status="pending",
            current_step=0,
            attempts=0,
            sleep_until=gate,
            step_outcomes=[],
            last_error=None,
            created_at=now,
            updated_at=now,
            started_at=None,
            finished_at=None,
        )
        self._workflows.append(run)
        return run.id

    def _find_workflow(self, run_id: str) -> _WorkflowRun | None:
        for r in self._workflows:
            if r.id == run_id:
                return r
        return None

    def _advance_workflows(self, now: int) -> None:
        """One claim pass per tick (mirroring the server's scheduler poll
        cadence): every due pending OR waiting run flips to running
        (``startedAt`` stamped on the first claim only), then each advances
        through :meth:`_advance_run`. A claimed ``waiting`` row means the wait
        timed out — or was woken by a delivery, which flips the row to
        ``pending`` first (``signal_workflow``). A run that a sibling's step
        cancelled mid-pass is skipped by the in-loop status re-check."""
        due = [
            r
            for r in self._workflows
            if r.status in ("pending", "waiting")
            and (r.sleep_until is None or r.sleep_until <= now)
        ]
        for run in due:
            # Re-resolve from the live store: a sibling run's step txn may have
            # cancelled (or rolled back a cancel of) this one, and a failed
            # sibling txn replaces self._workflows with its snapshot — the
            # ``due`` reference would then read stale state.
            live = self._find_workflow(run.id)
            if live is None or live.status not in ("pending", "waiting"):
                continue
            live.status = "running"
            if live.started_at is None:
                live.started_at = now
            live.updated_at = now
            self._advance_run(live, now)

    def _advance_run(self, run: _WorkflowRun, now: int) -> None:
        """Advance one claimed run — the port of the committer's
        ``handle_workflow_advance`` loop. Re-checks the status at every loop
        boundary (only a running run continues — a cancel between steps stops
        advancement), and branches per step kind: an ``awaitSignal`` step takes
        the side-table path (consume a delivered signal / park / time out —
        no document writes), anything else executes the step's txn atomically.
        On txn success the run either moves to the next step (gating on its
        ``sleepBeforeMs``; a future gate releases to pending, an immediate one
        keeps looping in this same turn) or finalizes. On failure, retries with
        exponential backoff until ``maxAttempts`` is exhausted, then marks the
        run failed with the last error and a terminal failed outcome."""
        run_id = run.id
        while True:
            # Re-resolve from the live store at every boundary — the server
            # re-reads the row's status each loop iteration
            # (``workflows::status_of``), and here a failed step txn restores
            # ``self._workflows`` from its deepcopy snapshot, detaching any
            # prior reference. Mutations must land on the live object.
            live = self._find_workflow(run_id)
            if live is None or live.status != "running":
                return
            run = live
            step = run.spec.steps[run.current_step]
            retry = step.retry or _DEFAULT_STEP_RETRY
            if step.await_signal is not None:
                if self._advance_await_signal(run, step.await_signal, retry, now):
                    continue
                return
            try:
                txn = Transaction.model_validate(step.txn)
                self._execute_transaction(txn)
            except RtDbError as err:
                # The failed txn's rollback replaced the store with its
                # snapshot — re-resolve so attempts/backoff hit the live row.
                restored = self._find_workflow(run_id)
                if restored is None:
                    return
                run = restored
                run.attempts += 1
                run.updated_at = now
                if run.attempts < retry.max_attempts:
                    backoff = min(
                        retry.initial_retry_ms * (2 ** min(run.attempts - 1, 32)),
                        retry.max_retry_ms,
                    )
                    run.sleep_until = now + backoff
                    run.status = "pending"
                    return
                run.status = "failed"
                run.last_error = err.message
                run.finished_at = now
                run.step_outcomes.append(
                    StepOutcome(
                        step_index=run.current_step,
                        status="failed",
                        attempts=run.attempts,
                        at=now,
                        error=err.message,
                    )
                )
                return
            run.step_outcomes.append(
                StepOutcome(
                    step_index=run.current_step,
                    status="success",
                    attempts=run.attempts + 1,
                    at=now,
                )
            )
            run.attempts = 0
            run.last_error = None
            run.updated_at = now
            if run.current_step == run.step_count - 1:
                run.status = "success"
                run.finished_at = now
                return
            run.current_step += 1
            gate = now + (run.spec.steps[run.current_step].sleep_before_ms or 0)
            if gate > now:
                run.sleep_until = gate
                run.status = "pending"
                return

    def _advance_await_signal(
        self, run: _WorkflowRun, sig: AwaitSignalSpec, retry: StepRetry, now: int
    ) -> bool:
        """One boundary of an ``awaitSignal`` step — the port of the committer's
        three-way branch (side-table only: no document writes, no subscriber
        notifications). The claimed row itself is the wake discriminator:
        ``signal_payload`` set = a delivery to consume; else ``waited_since``
        unset = first arrival (park); set = the gate expired (a timed-out
        attempt). A timed-out retry waits the FULL ``timeoutMs`` again — never
        backoff — and exhaustion terminal-fails with
        ``awaitSignal '<name>' timed out``. Returns ``True`` when the advance
        loop should continue in this same turn (a consumed signal whose next
        step's gate is already due), ``False`` when the run parked, timed out
        into a retry, or finalized."""
        if run.signal_payload is not _NO_SIGNAL:
            payload = run.signal_payload
            run.signal_payload = _NO_SIGNAL
            run.step_outcomes.append(
                StepOutcome(
                    step_index=run.current_step,
                    status="success",
                    attempts=run.attempts + 1,
                    at=now,
                    signal=payload,
                )
            )
            run.attempts = 0
            run.last_error = None
            run.wait_name = None
            run.waited_since = None
            run.updated_at = now
            if run.current_step == run.step_count - 1:
                run.status = "success"
                run.finished_at = now
                return False
            run.current_step += 1
            gate = now + (run.spec.steps[run.current_step].sleep_before_ms or 0)
            if gate > now:
                run.sleep_until = gate
                run.status = "pending"
                return False
            return True
        # An omitted timeoutMs is never due — only a delivery or a cancel
        # wakes the run.
        timeout_gate = now + sig.timeout_ms if sig.timeout_ms is not None else _NEVER_DUE
        if run.waited_since is None:
            # First arrival: park (attempts persist, so a timeout retry that
            # re-parks keeps its count).
            run.status = "waiting"
            run.wait_name = sig.name
            run.waited_since = now
            run.sleep_until = timeout_gate
            run.signal_payload = _NO_SIGNAL
            run.updated_at = now
            return False
        # The run parked and its gate expired: a timed-out attempt. A retry
        # waits the FULL timeoutMs again — never backoff.
        run.attempts += 1
        run.updated_at = now
        if run.attempts < retry.max_attempts:
            run.status = "waiting"
            run.wait_name = sig.name
            run.waited_since = now
            run.sleep_until = timeout_gate
            run.signal_payload = _NO_SIGNAL
            return False
        error = f"awaitSignal '{sig.name}' timed out"
        run.status = "failed"
        run.last_error = error
        run.finished_at = now
        run.wait_name = None
        run.waited_since = None
        run.signal_payload = _NO_SIGNAL
        run.step_outcomes.append(
            StepOutcome(
                step_index=run.current_step,
                status="failed",
                attempts=run.attempts,
                at=now,
                error=error,
            )
        )
        return False

    def _reap_ttl(self, now: int) -> int:
        """Remove docs whose declared TTL ``field`` (a number) is ``< now`` — the
        in-memory mirror of the server's per-tick TTL reaper. Fires only on
        tables that declare ``ttl``; non-numeric or absent values are left alone.
        Notifies subscribers on each touched table so reactive subscriptions see
        the expiry as a delete. Returns the count of removed docs.

        FM-33: the reaper ALWAYS hard-deletes (``force_hard`` — even on a
        ``softDelete`` table; the reaper is the purge mechanism), and when some
        table declares an ``onDelete`` ref targeting the reaped table the expiry
        runs through :meth:`_delete_row_cascade` so children follow their
        declared action. Mirror of server ``handle_reaper``'s bulk-vs-cascade
        branch: ``visited`` is shared across the sweep (a row cascaded by an
        earlier expiry is skipped) while the budget is fresh per initiating
        row; a failing row is skipped and retried on the next sweep, not fatal."""
        touched: set[str] = set()
        removed = 0
        # Shared across the whole sweep: a row already hard-deleted (or
        # stamped) by an earlier expiry's cascade is skipped, not an error.
        # Locally scoped so a failed row retries on the NEXT sweep.
        sweep_visited: set[tuple[str, str]] = set()
        # Snapshot the items — popping mid-iteration would skip rows.
        for (table, doc_id), row in list(self._docs.items()):
            tdef = self._tables.get(table)
            if tdef is None or tdef.ttl is None:
                continue
            value = row.doc.get(tdef.ttl.field)
            if isinstance(value, (int, float)) and value < now:
                if any(
                    _on_delete_ref(ft, table) is not None
                    for other in self._tables.values()
                    for ft in other.fields.values()
                ):
                    try:
                        self._delete_row_cascade(table, doc_id, sweep_visited, [0], True, touched)
                    except RtDbError:
                        # Per-row failures are skipped and retried next sweep
                        # (at-least-once, like the server's warn-and-continue);
                        # cascade work before the failure stays, as server-side.
                        continue
                else:
                    self._docs.pop((table, doc_id), None)
                    touched.add(table)
                removed += 1
        if touched:
            self._notify_subs(touched)
        return removed

    def tick(self, now_ms: int | None = None) -> None:
        """Advance the harness clock to ``now_ms`` (or the client clock when
        omitted), then (1) reap docs whose TTL field is in the past and (2) fire
        every due non-paused scheduled job by applying its txn through the same
        atomic path as :meth:`mutate` (so reactive subscriptions see the write).
        One-shots are removed after a successful fire; crons re-arm by
        :data:`CRON_STEP_MS` and interval jobs by their ``everyMs`` (missed
        windows are skipped, never backfilled). A job whose txn fails is marked
        ``error`` but left in place (recurring kinds re-arm), so a subsequent
        ``tick`` retries it. External jobs are NEVER fired — the worker that
        claims them over HTTP owns their execution.

        Workflows (FM-29): after schedules, one claim pass advances every due
        pending run (see :meth:`_advance_workflows`)."""
        now = now_ms if now_ms is not None else self._now()
        self._reap_ttl(now)
        self._advance_workflows(now)
        i = 0
        while i < len(self._schedules):
            job = self._schedules[i]
            # External jobs are never internally executed (server claim_due/
            # next_due exclude external rows) — tick skips them so the job
            # stays pending for its claiming worker.
            if job.status == "paused" or job.external or job.due_at > now:
                i += 1
                continue
            txn = job.txn
            job_id = job.id
            kind = job.kind
            try:
                self._execute_transaction(txn)
            except RtDbError as err:
                j = self._find_job(job_id)
                if j is not None:
                    j.status = "error"
                    j.last_error = err.message
                    if kind == "cron":
                        j.due_at = now + CRON_STEP_MS
                    elif kind == "interval" and j.every_ms is not None:
                        # Error path re-arms too (server reschedule_recurring_error).
                        j.due_at = now + j.every_ms
            else:
                j = self._find_job(job_id)
                if j is not None:
                    prev_due_at = j.due_at
                    j.fired_count += 1
                    if kind == "oneshot":
                        # Remove after a successful fire; don't bump i (the next
                        # job shifts into this index).
                        self._schedules = [s for s in self._schedules if s.id != job_id]
                        continue
                    if kind == "interval" and j.every_ms is not None:
                        # The elapsed-window count is exact for interval jobs
                        # (unlike cron, which this harness approximates on a
                        # fixed CRON_STEP_MS rather than real cron math).
                        delta = now - prev_due_at
                        if delta > 0:
                            missed = (delta - 1) // j.every_ms
                            if missed > 0:
                                j.missed_count += missed
                                j.last_missed_at = now
                        j.due_at = now + j.every_ms
                    else:
                        j.due_at = now + CRON_STEP_MS
                    j.status = "pending"
            i += 1

    def _find_job(self, job_id: str) -> _ScheduledJob | None:
        for j in self._schedules:
            if j.id == job_id:
                return j
        return None

    def _due_at_for(self, when: ScheduleWhen, now: int) -> int:
        match when:
            case AfterMs(ms=ms):
                return now + ms
            case RunAt(ms=ms):
                return ms
            case Interval(every_ms=every_ms):
                return now + every_ms
            case _:
                return now + CRON_STEP_MS  # cron

    def upload(self, data: bytes, content_type: str | None = None) -> UploadResult:
        """Store ``data`` and return a server-shaped :class:`UploadResult`. The
        id is a short counter-prefixed token (distinct in shape from document ids)."""
        self._id_counter += 1
        new_id = f"f{_base36(self._id_counter)}"
        digest = _sha256_hex(data)
        size = len(data)
        created_at = self._now()
        self._storage[new_id] = StoredBlob(
            bytes=data,
            content_type=content_type,
            created_at=created_at,
            sha256=digest,
        )
        return UploadResult(id=new_id, sha256=digest, size=size, content_type=content_type)

    def delete_file(self, id: str) -> None:
        """Delete a stored blob. ``NOT_FOUND`` if unknown."""
        if self._storage.pop(id, None) is None:
            raise RtDbError(ErrorCode.NOT_FOUND, "unknown file")

    def get_file_metadata(self, id: str) -> FileMetadata:
        """Read back a stored blob's metadata (``sha256`` is empty — only the
        upload result carries the real digest). ``NOT_FOUND`` if unknown."""
        blob = self._storage.get(id)
        if blob is None:
            raise RtDbError(ErrorCode.NOT_FOUND, "unknown file")
        return FileMetadata(
            id=id,
            sha256="",
            size=len(blob.bytes),
            content_type=blob.content_type,
            creation_time=blob.created_at,
        )

    def get_url(self, id: str) -> str:
        """Synthetic handle — no real byte stream."""
        return f"memory://{id}"

    def transform_url(
        self,
        id: str,
        *,
        w: int | None = None,
        h: int | None = None,
        fit: Literal["cover", "contain", "scale-down"] | None = None,
        q: int | None = None,
        format: Literal["jpeg", "png", "auto"] | None = None,
    ) -> str:
        """Synthetic handle with image-transform params (ENH-014). No real byte stream.

        Params appear in the deterministic order ``w, h, fit, q, format``; unset
        params (and ``format="auto"``, the server default) are omitted. Mirrors
        ``RtDbHttpClient.transform_url`` so tests against the in-memory harness
        assert the same query-string shape.
        """
        parts: list[str] = []
        if w is not None:
            parts.append(f"w={w}")
        if h is not None:
            parts.append(f"h={h}")
        if fit is not None:
            parts.append(f"fit={fit}")
        if q is not None:
            parts.append(f"q={q}")
        # "auto" is the server default — omit so the URL stays minimal (rust parity).
        if format is not None and format != "auto":
            parts.append(f"format={format}")
        base = f"memory://{id}"
        return f"{base}?{'&'.join(parts)}" if parts else base
