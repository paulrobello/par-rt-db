"""In-memory par-rt-db client for unit tests. No network, no Postgres; mirrors
server DSL/step-result/system-field semantics. Ports ``rust-client/src/in_memory.rs``
(and through it ``ts-client/src/in_memory.ts``).

The server (``server/src/{txn,query,schema,protocol}.rs``) is the source of truth
for the declarative DSL, step-result shapes, system fields, and query semantics;
this module mirrors them so app code can exercise query/txn/schema behavior with
no network and no live Postgres. It exposes the same data surface as the live
clients — :meth:`push_schema`, :meth:`run_query` (one-shot), :meth:`mutate`
(transactions), :meth:`subscribe` (reactive ``queryUpdate``-style results), and
:meth:`tick` (advance scheduled jobs) — so a test can swap it in behind a shared
interface.

Parity is deliberately scoped to the documented core (schema push, insert /
patch / replace / delete / expectVersion / expectAbsent / upsert, point reads,
index eq + range queries with order/take/unique/first/count, filter expressions,
keyset-cursor pagination, reactive subscriptions, and scheduled-job ``tick``).
``distinct``/``aggregate``/``vectorSearch``/``hybridSearch`` are NOT implemented:
the first two raise :class:`RtDbError` ``BAD_REQUEST`` (stricter than the Rust
harness, which silently falls through to ``collect`` for them). ``vectorSearch``
applies its optional ``filter`` but does not rank by vector similarity (every
table row is a candidate — the sound over-approximation); ``hybridSearch``
returns an empty list after the same combination guards the server enforces.
``search`` applies its optional ``filter`` (every table row is a candidate —
ts_rank is not modeled) but otherwise does not rank.

Simplifications vs. the live server (be explicit when relying on these):

* Cron validation is deferred to the live server; the harness accepts any
  expression and re-arms crons by a fixed :data:`CRON_STEP_MS` interval (it does
  not parse 5-field cron). One-shots catch up if past due; crons skip missed
  windows (they do not backfill).
* Storage is an in-memory ``bytes`` map; :meth:`get_url` returns a synthetic
  ``memory://`` handle (there is no real byte stream to serve).
* Subscription callbacks fire inline on the writing thread; never recursively
  mutate the same client from inside a callback.
* Unsubscription is explicit (:meth:`SubscriptionHandle.unsubscribe`) or via a
  context manager (``with client.subscribe(...) as sub:``). The Rust RAII
  "dropping the handle unsubscribes" idiom is not relied on here — Python's GC
  is not deterministic, so prefer the explicit form.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any, Literal

from pydantic import TypeAdapter

from .cursor import decode_cursor, encode_cursor
from .errors import ErrorCode, RtDbError
from .migration import (
    Cast,
    Directive,
    _ChangeType,
    _DropField,
    _DropIndex,
    _DropTable,
    _EvalExpr,
    _RenameField,
    _RenameTable,
    _SetDefault,
)
from .mutation import (
    Step,
    StepResult,
    Transaction,
    _Delete,
    _DeleteByQuery,
    _ExpectAbsent,
    _ExpectVersion,
    _Insert,
    _Patch,
    _PatchByQuery,
    _Replace,
    _Upsert,
)
from .query import Query, _terminal_of, parse_result
from .schema import (
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
from .wire import (
    AfterMs,
    AggregateOp,
    AuthedUser,
    Cron,
    FilterExpr,
    PresenceMember,
    RunAt,
    ScheduleInfo,
    ScheduleWhen,
    _FilterAnd,
    _FilterContains,
    _FilterEq,
    _FilterExists,
    _FilterGt,
    _FilterGte,
    _FilterIn,
    _FilterLt,
    _FilterLte,
    _FilterNeq,
    _FilterNot,
    _FilterOr,
)

#: Maximum number of steps in a single transaction (mirrors the server cap).
MAX_STEPS = 256
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
#: Approximate cron re-fire interval for the in-memory stub. Real 5-field cron
#: parsing is deferred to the server; the harness only needs crons to re-arm.
CRON_STEP_MS = 60_000


def worst_case_affected(txn: Transaction) -> int:
    """SEC-104: total documents a txn could touch in the worst case.

    Per-id steps count 1 each; each ``patchByQuery``/``deleteByQuery`` step
    counts up to its ``limit`` (default and cap ``MAX_BY_QUERY_ROWS``). Mirrors
    server ``txn::worst_case_affected``; used by ``_execute_transaction``'s
    ``MAX_AFFECTED_ROWS_PER_TXN`` budget check.
    """
    total = 0
    for step in txn.steps:
        if isinstance(step, (_PatchByQuery, _DeleteByQuery)):
            limit_opt = step.limit
            total += MAX_BY_QUERY_ROWS if limit_opt is None else min(limit_opt, MAX_BY_QUERY_ROWS)
        else:
            total += 1
    return total


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
# ``{"id","inserted"}`` -> upsert result.
_STEP_RESULT = TypeAdapter(StepResult)


# ---------------------------------------------------------------------------
# Stored rows / jobs / blobs
# ---------------------------------------------------------------------------


@dataclass
class StoredRow:
    """A stored row: the user doc plus its identity/history, kept separate so the
    system fields (``_id``/``_creationTime``/``_version``) are merged in only at
    read time — exactly as the server stores ``doc`` jsonb alongside ``id``/
    ``created_at``/``version`` columns."""

    id: str
    doc: dict[str, Any]
    version: int
    created_at: int


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
    jobs by applying ``txn`` through the same atomic path as :meth:`mutate`."""

    id: str
    kind: str  # "oneshot" | "cron"
    txn: Transaction
    due_at: int
    cron: str | None
    status: str  # "pending" | "paused" | "error"
    created_at: int
    fired_count: int
    last_error: str | None


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


# ---- ENH-015 presence (shared in-memory backing) ---------------------------
#
# Ports ``PresenceRooms`` from ``ts-client/src/in_memory.ts`` (and through it
# ``rust-client/src/in_memory.rs``). A ``room → connectionId → member`` map with
# a per-room subscriber list: two :class:`InMemoryRtDbClient` instances that
# share a :class:`PresenceRooms` see each other's joins/updates/leaves fan out,
# approximating the server's per-db presence registry for tests (one client =
# one connection, keyed by ``connectionId``). A client with no ``presence_rooms``
# option gets a private instance and only ever sees itself.


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


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------


class InMemoryRtDbClient:
    """In-memory par-rt-db client for unit tests.

    Construct with :class:`InMemoryRtDbClientOptions` (defaults: system clock,
    constant ``0.5`` RNG), then :meth:`push_schema` a schema and drive it with
    :meth:`run_query` / :meth:`mutate` / :meth:`subscribe` / :meth:`tick`.
    """

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
        # Counter for storage-upload id minting (also seeds connection_id when
        # not injected — mirrors the TS harness's `c{N}` default).
        self._id_counter: int = 0
        # mut_id -> cached results (idempotency short-circuit).
        self._idempotency: dict[str, list[StepResult]] = {}
        # Scheduled jobs (one-shot + cron).
        self._schedules: list[_ScheduledJob] = []
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

    # ---- schema ---------------------------------------------------------

    def push_schema(self, schema: SchemaDef) -> None:
        """Install ``schema`` as this client's sole in-memory database schema,
        merging additively on subsequent pushes: existing docs and idempotency
        entries are preserved, and ``_tables`` is repopulated from the new schema
        (folding in new fields/indexes/tables without touching rows). Destructive
        changes — a removed/changed table, field, or index — raise
        :class:`RtDbError` ``BAD_REQUEST`` with the same messages as the live
        server's ``ddl.rs::detect_destructive_changes``."""
        if self._schema is not None:
            _detect_destructive_changes(self._schema, schema)
        self._schema = schema
        for name, def_ in schema.tables.items():
            self._tables[name] = def_

    def to_schema_json(self) -> SchemaDef | None:
        """Snapshot of the currently-installed schema (or ``None`` before
        :meth:`push_schema`)."""
        return self._schema

    def migrate_schema(
        self,
        directives: list[Directive],
        *,
        dry_run: bool = False,
    ) -> Any:
        """Apply (or preview) a declarative schema migration in-memory.

        Ports ``rust-client::InMemoryRtDbClient::migrate_schema`` and through it
        ``server::migrate::plan_migration`` + ``apply_migration``. Each directive
        is validated against the working schema fold and applied to the doc
        store. On the first failure the doc store is restored (``self._schema``
        was never touched — the fold lives in a local ``planned`` copy) and the
        error surfaces. On ``dry_run`` the full plan is validated and
        ``affected_rows`` reported against the derived schema, but nothing is
        committed (``applied: False``).

        ``evalExpr`` has no in-memory SQL engine and raises
        :class:`RtDbError` ``BAD_REQUEST`` — same convention as the
        search/vector stubs. Affected-rows counts mirror the server:
        ``renameField`` / ``setDefault`` / ``changeType`` / ``dropField`` count
        the rows whose docs actually changed; ``dropTable`` counts every row
        (all deleted); ``renameTable`` / ``dropIndex`` report zero.

        Returns a :class:`par_rt_db.http_client.MigrateResult` (imported lazily
        to avoid a circular dependency at module load time).
        """
        from .http_client import DirectiveReport, MigrateResult

        if self._schema is None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "no schema pushed for migration")

        planned = deepcopy(self._schema)
        snapshot = deepcopy(self._docs)
        touched: set[str] = set()
        reports: list[DirectiveReport] = []

        for d in directives:
            try:
                report, table = self._apply_migration_directive(planned, d)
            except RtDbError:
                self._docs = snapshot
                raise
            reports.append(report)
            if table is not None:
                touched.add(table)

        if dry_run:
            self._docs = snapshot
            return MigrateResult(applied=False, schema=planned, directives=reports)

        self._schema = planned
        self._tables.clear()
        for name, def_ in planned.tables.items():
            self._tables[name] = def_
        self._notify_subs(touched)
        return MigrateResult(applied=True, schema=planned, directives=reports)

    def _apply_migration_directive(
        self,
        planned: SchemaDef,
        d: Directive,
    ) -> tuple[Any, str | None]:
        """Apply one directive to the working ``planned`` schema and ``self._docs``.

        Returns ``(DirectiveReport, Optional[table_name])`` where the table name
        marks the directive's touched table for subscription re-run. Mirrors
        ``rust-client::InMemoryRtDbClient::apply_migration_directive``.
        """

        if isinstance(d, _RenameField):
            return self._migrate_rename_field(planned, d), d.table
        if isinstance(d, _RenameTable):
            return self._migrate_rename_table(planned, d), d.to
        if isinstance(d, _ChangeType):
            return self._migrate_change_type(planned, d), d.table
        if isinstance(d, _DropField):
            return self._migrate_drop_field(planned, d), d.table
        if isinstance(d, _DropTable):
            return self._migrate_drop_table(planned, d), d.name
        if isinstance(d, _DropIndex):
            return self._migrate_drop_index(planned, d), d.table
        if isinstance(d, _SetDefault):
            return self._migrate_set_default(planned, d), d.table
        if isinstance(d, _EvalExpr):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"evalExpr unsupported in-memory (table '{d.table}')",
            )
        # Exhaustive over the Directive union — if a new variant is added,
        # pyright flags this fallback as unreachable. Do not collapse.
        raise RtDbError(ErrorCode.INTERNAL, f"unknown migration directive: {d!r}")

    def _migrate_rename_field(
        self,
        planned: SchemaDef,
        d: _RenameField,
    ) -> Any:
        from .http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        if d.to in t.fields:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"rename target '{d.table}.{d.to}' already exists",
            )
        ft = t.fields.pop(d.from_, None)
        if ft is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"renamed field '{d.table}.{d.from_}' does not exist",
            )
        t.fields[d.to] = ft
        for ix in t.indexes:
            ix.fields = [d.to if f == d.from_ else f for f in ix.fields]
        if t.owner_field == d.from_:
            t.owner_field = d.to
        if t.collaborators_field == d.from_:
            t.collaborators_field = d.to
        affected = 0
        for (tname, _), row in self._docs.items():
            if tname != d.table:
                continue
            if d.from_ in row.doc:
                row.doc[d.to] = row.doc.pop(d.from_)
                affected += 1
        return DirectiveReport(op="renameField", affected_rows=affected)

    def _migrate_rename_table(
        self,
        planned: SchemaDef,
        d: _RenameTable,
    ) -> Any:
        from .http_client import DirectiveReport

        if d.to in planned.tables:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"rename target table '{d.to}' already exists",
            )
        def_ = planned.tables.pop(d.from_, None)
        if def_ is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"renamed table '{d.from_}' does not exist",
            )
        # Id references to `from_` in other tables follow the rename.
        for other in planned.tables.values():
            for ft in other.fields.values():
                if isinstance(ft, _FId) and ft.table == d.from_:
                    ft.table = d.to
        planned.tables[d.to] = def_
        # Re-key the live doc store: (from_, id) → (to, id).
        keys_to_move = [k for k in self._docs if k[0] == d.from_]
        for k in keys_to_move:
            row = self._docs.pop(k)
            self._docs[(d.to, k[1])] = row
        return DirectiveReport(op="renameTable", affected_rows=0)

    def _migrate_change_type(
        self,
        planned: SchemaDef,
        d: _ChangeType,
    ) -> Any:
        from .http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        old_ty = t.fields.get(d.field)
        if old_ty is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"changed field '{d.table}.{d.field}' does not exist",
            )
        if not _cast_valid_for(d.cast, old_ty):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"cast {d.cast.value} is not valid for {d.table}.{d.field}",
            )
        affected = 0
        for (tname, row_id), row in self._docs.items():
            if tname != d.table:
                continue
            val = row.doc.get(d.field)
            if val is None:
                continue
            affected += 1
            coerced = _coerce_value(d.cast, val)
            if coerced is not None:
                row.doc[d.field] = coerced
            elif d.default is not None:
                dv = _coerce_value(d.cast, d.default)
                row.doc[d.field] = dv if dv is not None else d.default
            else:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changeType cannot coerce value in {d.table}.{row_id} ({val}) "
                    "and no default given",
                )
        t.fields[d.field] = d.to
        return DirectiveReport(op="changeType", affected_rows=affected)

    def _migrate_drop_field(
        self,
        planned: SchemaDef,
        d: _DropField,
    ) -> Any:
        from .http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        if t.fields.pop(d.field, None) is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"dropped field '{d.table}.{d.field}' does not exist",
            )
        for ix in t.indexes:
            ix.fields = [f for f in ix.fields if f != d.field]
        if t.owner_field == d.field:
            t.owner_field = None
        if t.collaborators_field == d.field:
            t.collaborators_field = None
        affected = 0
        for (tname, _), row in self._docs.items():
            if tname != d.table:
                continue
            if d.field not in row.doc:
                continue
            row.doc.pop(d.field, None)
            affected += 1
        return DirectiveReport(op="dropField", affected_rows=affected)

    def _migrate_drop_table(
        self,
        planned: SchemaDef,
        d: _DropTable,
    ) -> Any:
        from .http_client import DirectiveReport

        if planned.tables.pop(d.name, None) is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"dropped table '{d.name}' does not exist",
            )
        keys_to_remove = [k for k in self._docs if k[0] == d.name]
        for k in keys_to_remove:
            del self._docs[k]
        return DirectiveReport(op="dropTable", affected_rows=len(keys_to_remove))

    def _migrate_drop_index(
        self,
        planned: SchemaDef,
        d: _DropIndex,
    ) -> Any:
        from .http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        if not any(ix.name == d.name for ix in t.indexes):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"dropped index '{d.table}.{d.name}' does not exist",
            )
        t.indexes = [ix for ix in t.indexes if ix.name != d.name]
        return DirectiveReport(op="dropIndex", affected_rows=0)

    def _migrate_set_default(
        self,
        planned: SchemaDef,
        d: _SetDefault,
    ) -> Any:
        from .http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        if d.field not in t.fields:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"setDefault target '{d.table}.{d.field}' does not exist",
            )
        affected = 0
        for (tname, _), row in self._docs.items():
            if tname != d.table:
                continue
            if d.field not in row.doc:
                row.doc[d.field] = d.value
                affected += 1
        return DirectiveReport(op="setDefault", affected_rows=affected)

    def get(self, table: str, id: str) -> dict[str, Any] | None:
        """Minimal point read — the merged doc (system fields included) for
        ``(table, id)``, or ``None`` if absent."""
        row = self._docs.get((table, id))
        return None if row is None else _merge_doc(row)

    def collect_all(self, table: str) -> list[dict[str, Any]]:
        """Test/debug helper — every merged doc in ``table``, in unspecified
        order. Not part of the query DSL."""
        return [_merge_doc(row) for (t, _), row in self._docs.items() if t == table]

    # ---- query ----------------------------------------------------------

    def run_query(self, q: Query) -> Any:
        """Execute a one-shot query. Returns the terminal result:

        * ``get(id)`` / ``first`` → merged doc, or ``None`` when absent.
        * ``unique`` → merged doc, ``None`` when zero match, or
          :class:`RtDbError` ``PRECONDITION_FAILED`` when more than one matches.
        * ``count`` → ``int``.
        * ``take`` / ``collect`` → ``list`` of merged docs.
        * ``paginate`` → ``{"docs": [...], "nextCursor"?: str}``.
        * ``search`` → list of merged docs narrowed by the terminal's optional
          ``filter`` (ranking is not modeled — every table row is a candidate).
        * ``vectorSearch`` → list of merged docs narrowed by the terminal's
          optional ``filter`` (vector similarity is not modeled — every table
          row is a candidate; ``hybridSearch`` still returns an empty list).

        ``filter`` is structurally validated once up front, then evaluated per
        row. See the module docs for the unimplemented terminals.
        """
        table_def = self._require_table(q.table)
        eq = q.eq or []
        has_range = q.gt is not None or q.gte is not None or q.lt is not None or q.lte is not None
        unique = bool(q.unique)
        first = bool(q.first)
        count = bool(q.count)

        # `get` terminal — exclusive of every other clause.
        if q.get is not None:
            return self._execute_get_terminal(q, eq, has_range)

        # Conflicting-terminal guards.
        if unique and (
            q.take is not None or q.order is not None or q.distinct or q.aggregate is not None
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "unique cannot be combined with take, order, distinct, or aggregate",
            )
        if first and unique:
            raise RtDbError(ErrorCode.BAD_REQUEST, "first cannot be combined with unique")
        if first and q.take is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "first cannot be combined with take")
        if first and q.distinct:
            raise RtDbError(ErrorCode.BAD_REQUEST, "first cannot be combined with distinct")
        if first and q.aggregate is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "first cannot be combined with aggregate")
        if count and unique:
            raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with unique")
        if count and q.take is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with take")
        if count and first:
            raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with first")
        if count and q.order is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with order")
        if count and q.distinct:
            raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with distinct")
        if count and q.aggregate is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with aggregate")
        if q.paginate is not None:
            if count:
                raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with count")
            if unique:
                raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with unique")
            if first:
                raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with first")
            if q.take is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with take")
            if q.distinct:
                raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with distinct")
            if q.aggregate is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with aggregate")
        if q.gt is not None and q.gte is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "gt and gte cannot both be set")
        if q.lt is not None and q.lte is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "lt and lte cannot both be set")
        if q.take is not None and q.take > MAX_TAKE:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"take exceeds maximum of {MAX_TAKE}")

        # `distinct`/`aggregate` are standalone terminals (like `count`): they
        # compose only with index/eq/range/filter. `get`/`unique`/`first`/`count`
        # rejected their own combinations above (validated first, matching the
        # server's check order), so these blocks reject the remaining peers each
        # terminal owns — mirroring the server's DISTINCT/AGGREGATE_INCOMPATIBLES.
        if q.distinct:
            if q.take is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with take")
            if q.order is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with order")
            if q.aggregate is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with aggregate")
            if q.paginate is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with paginate")
            if q.search is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with search")
            if q.vector_search is not None:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST, "distinct cannot be combined with vector search"
                )
            if q.hybrid_search is not None:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST, "distinct cannot be combined with hybrid search"
                )
        if q.aggregate is not None:
            if q.take is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "aggregate cannot be combined with take")
            if q.order is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "aggregate cannot be combined with order")
            if q.paginate is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "aggregate cannot be combined with paginate")
            if q.search is not None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "aggregate cannot be combined with search")
            if q.vector_search is not None:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST, "aggregate cannot be combined with vector search"
                )
            if q.hybrid_search is not None:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST, "aggregate cannot be combined with hybrid search"
                )

        # `vectorSearch` terminal — no in-memory vector ranking; every row in
        # the table is treated as a candidate (the sound over-approximation — a
        # real match can never be excluded, mirroring the `search` stub's
        # treatment of ts_rank). A declared `filter` narrows that candidate set
        # via the same `_eval_filter_expr` the db-side `.filter()` uses, so the
        # narrowing path is exercised end-to-end without modeling vector
        # similarity. The terminal's `limit` is not applied: without ranking
        # there is no meaningful "top N" to pick.
        if q.vector_search is not None:
            return self._execute_vector_search_terminal(q, table_def, eq, has_range)
        if q.search is not None:
            if (
                q.index is not None
                or eq
                or has_range
                or q.order is not None
                or unique
                or first
                or count
                or q.filter is not None
                or q.vector_search is not None
                or q.paginate is not None
                or q.hybrid_search is not None
            ):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    "search cannot be combined with index, eq, range bounds, order, "
                    "unique, first, count, filter, vector search, paginate, or hybrid search",
                )
            # Full-text ranking (tsvector match + ts_rank) is not modeled
            # in-memory; every row in the table is treated as a candidate (the
            # sound over-approximation — a real match can never be excluded). A
            # declared `filter` narrows that candidate set via the same
            # `_eval_filter_expr` the db-side `.filter()` uses, so the narrowing
            # path is exercised end-to-end without modeling ts_rank.
            if q.search.filter is not None:
                _validate_filter(q.search.filter, set(table_def.fields.keys()))
            candidates: list[StoredRow] = [
                row for (t, _id), row in self._docs.items() if t == q.table
            ]
            if q.search.filter is not None:
                candidates = [
                    row for row in candidates if _eval_filter_expr(q.search.filter, row.doc)
                ]
            return [_merge_doc(row) for row in candidates]

        # `hybridSearch` terminal — standalone like `vectorSearch`: rejects every
        # peer. RRF ranking is not modeled in-memory, so a valid (peer-free)
        # hybridSearch returns an empty list (the sound stub — the combination
        # guards the server enforces are still exercised).
        if q.hybrid_search is not None:
            if (
                q.index is not None
                or eq
                or has_range
                or q.order is not None
                or unique
                or first
                or count
                or q.distinct
                or q.aggregate is not None
                or q.paginate is not None
                or q.filter is not None
                or q.search is not None
                or q.vector_search is not None
                or q.take is not None
            ):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    "hybridSearch cannot be combined with any other terminal",
                )
            return []

        # Resolve index — required for `eq` and for any range bound.
        index_def: IndexDef | None = None
        if q.index is not None:
            index_def = _require_index(table_def, q.index)
        elif eq:
            raise RtDbError(ErrorCode.BAD_REQUEST, "eq requires an index")

        # eq-arity check.
        if index_def is not None and len(eq) > len(index_def.fields):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"index '{index_def.name}' expects at most {len(index_def.fields)} "
                f"eq value(s), got {len(eq)}",
            )

        # Type-check each eq prefix bind positionally.
        typed_eq: list[Any] = []
        if index_def is not None:
            for i, value in enumerate(eq):
                typed_eq.append(_coerce_index_value(table_def, index_def.fields[i], value))

        # Range bounds apply to the next index field after the eq prefix.
        range_field: str | None = None
        if has_range:
            if index_def is None:
                raise RtDbError(ErrorCode.BAD_REQUEST, "range bound requires an index")
            if len(eq) >= len(index_def.fields):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    "range bound requires a remaining index field after eq",
                )
            range_field = index_def.fields[len(eq)]
        range_pg = _TEXT
        if range_field is not None:
            range_field_ty = table_def.fields.get(range_field)
            if range_field_ty is not None:
                try:
                    range_pg = _index_column_type(range_field_ty).pg
                except RtDbError:
                    range_pg = _TEXT
        gt = (
            _coerce_index_value(table_def, range_field, q.gt)
            if q.gt is not None and range_field
            else None
        )
        gte = (
            _coerce_index_value(table_def, range_field, q.gte)
            if q.gte is not None and range_field
            else None
        )
        lt = (
            _coerce_index_value(table_def, range_field, q.lt)
            if q.lt is not None and range_field
            else None
        )
        lte = (
            _coerce_index_value(table_def, range_field, q.lte)
            if q.lte is not None and range_field
            else None
        )

        # Compile the filter against the table's declared fields once up front.
        if q.filter is not None:
            _validate_filter(q.filter, set(table_def.fields.keys()))

        # Row fetch + filter (eq prefix -> range -> filter hook).
        filtered: list[StoredRow] = []
        for (t, _id), row in self._docs.items():
            if t != q.table:
                continue
            if index_def is not None:
                ok = True
                for i, tv in enumerate(typed_eq):
                    rv = row.doc.get(index_def.fields[i])
                    if rv is None or rv != tv:
                        ok = False
                        break
                if not ok:
                    continue
            if range_field is not None:
                v = row.doc.get(range_field)
                if v is None:
                    continue
                if gt is not None and _compare_index_values(v, gt, range_pg) <= 0:
                    continue
                if gte is not None and _compare_index_values(v, gte, range_pg) < 0:
                    continue
                if lt is not None and _compare_index_values(v, lt, range_pg) >= 0:
                    continue
                if lte is not None and _compare_index_values(v, lte, range_pg) > 0:
                    continue
            if q.filter is not None and not _eval_filter_expr(q.filter, row.doc):
                continue
            filtered.append(row)

        if count:
            return len(filtered)

        # `distinct` terminal: unique values of the index field immediately
        # after the eq prefix over the matching set, sorted ascending, capped by
        # MAX_TAKE. Nulls are skipped (mirror WHERE "<col>" IS NOT NULL).
        if q.distinct:
            if index_def is None or len(typed_eq) >= len(index_def.fields):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    "distinct requires an index field beyond the eq prefix",
                )
            field = index_def.fields[len(typed_eq)]
            field_pg = _pg_for_field(table_def, field)
            seen: set[str] = set()
            distinct_values: list[Any] = []
            for row in filtered:
                v = row.doc.get(field)
                if v is None:
                    continue
                key = _dedupe_key(v)
                if key not in seen:
                    seen.add(key)
                    distinct_values.append(v)
            distinct_values.sort(key=cmp_to_key(lambda a, b: _compare_index_values(a, b, field_pg)))
            return distinct_values[:MAX_TAKE]

        # `aggregate` terminal: <OP> over the index field after the eq prefix
        # (groupBy: group by that field, aggregate the next). `count` aggregates
        # rows, not a field — it consumes no aggregate index field (a scalar
        # `count` needs no index at all; a grouped `count` needs one index field
        # beyond the eq prefix to group by). Null agg values are skipped for the
        # field-bearing ops (SQL NULL semantics); an empty scalar set -> None
        # (count -> 0); groups are ordered by key asc and capped by MAX_TAKE.
        if q.aggregate is not None:
            agg = q.aggregate
            eq_len = len(typed_eq)
            # `count` aggregates rows and consumes no aggregate field.
            needs_field = agg.op != AggregateOp.COUNT

            # Resolve the group field: groupBy always needs one index field
            # beyond the eq prefix.
            if agg.group_by:
                if index_def is None or eq_len >= len(index_def.fields):
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        "aggregate groupBy requires an index field beyond the eq prefix",
                    )
                group_field = index_def.fields[eq_len]
            else:
                group_field = None

            # Resolve the aggregate field (count consumes none; the rest need
            # one beyond the eq prefix, or two when grouped).
            if needs_field:
                if index_def is None:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        "aggregate requires an index field beyond the eq prefix",
                    )
                if agg.group_by:
                    if eq_len + 1 >= len(index_def.fields):
                        raise RtDbError(
                            ErrorCode.BAD_REQUEST,
                            "aggregate groupBy requires two index fields beyond the eq prefix",
                        )
                    agg_field = index_def.fields[eq_len + 1]
                else:
                    if eq_len >= len(index_def.fields):
                        raise RtDbError(
                            ErrorCode.BAD_REQUEST,
                            "aggregate requires an index field beyond the eq prefix",
                        )
                    agg_field = index_def.fields[eq_len]
                agg_pg = _pg_for_field(table_def, agg_field)
                if agg.op in (AggregateOp.SUM, AggregateOp.AVG) and agg_pg not in (_NUMBER, _INT64):
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        f"aggregate op {agg.op} requires a numeric index field",
                    )
            else:
                agg_field = None
                agg_pg = _TEXT  # unused for count

            if group_field is not None:
                group_pg = _pg_for_field(table_def, group_field)
                groups: list[tuple[Any, list[Any]]] = []
                group_index: dict[str, int] = {}
                for row in filtered:
                    k = row.doc.get(group_field)
                    if k is None:
                        continue
                    key = _dedupe_key(k)
                    i = group_index.get(key)
                    if i is None:
                        i = len(groups)
                        group_index[key] = i
                        groups.append((k, []))
                    if agg_field is not None:
                        av = row.doc.get(agg_field)
                        if av is not None:
                            groups[i][1].append(av)
                    else:
                        # count: every row in the group counts (COUNT(*)).
                        groups[i][1].append(1)
                if agg.op == AggregateOp.COUNT:
                    out: list[dict[str, Any]] = [{"key": k, "value": len(vs)} for k, vs in groups]
                else:
                    out = [
                        {"key": k, "value": _apply_aggregate(agg.op, vs, agg_pg) if vs else None}
                        for k, vs in groups
                    ]
                out.sort(
                    key=cmp_to_key(lambda a, b: _compare_index_values(a["key"], b["key"], group_pg))
                )
                return out[:MAX_TAKE]
            # Scalar path: count returns the matching-row count (0 if none);
            # the field-bearing ops reduce their non-null agg values (None if empty).
            if agg.op == AggregateOp.COUNT:
                return len(filtered)
            assert agg_field is not None  # needs_field is True for every non-count op
            agg_values = [row.doc.get(agg_field) for row in filtered]
            agg_values = [v for v in agg_values if v is not None]
            if not agg_values:
                return None
            return _apply_aggregate(agg.op, agg_values, agg_pg)

        # Sort keys: unbound index fields (after the eq prefix), then
        # _creationTime, then _id. The unique id tiebreaker makes the order total.
        direction = q.order or "asc"
        unbound_fields: list[str] = (
            index_def.fields[len(typed_eq) :] if index_def is not None else []
        )
        sort_field_pgs: list[_PgType] = [_pg_for_field(table_def, f) for f in unbound_fields]

        def cmp(a: StoredRow, b: StoredRow) -> int:
            for i, fld in enumerate(unbound_fields):
                av = a.doc.get(fld)
                bv = b.doc.get(fld)
                c = _compare_index_values(av, bv, sort_field_pgs[i])
                if c != 0:
                    return _dir_order(c, direction)
            c = (a.created_at > b.created_at) - (a.created_at < b.created_at)
            if c != 0:
                return _dir_order(c, direction)
            return _dir_order((a.id > b.id) - (a.id < b.id), direction)

        filtered.sort(key=cmp_to_key(cmp))

        # `paginate` terminal: keyset-cursor paging over the sorted set.
        if q.paginate is not None:
            sort_cols: list[tuple[str, str | None]] = [
                *[("index", f) for f in unbound_fields],
                ("createdAt", None),
                ("id", None),
            ]
            col_types: list[_PgType] = [
                _pg_for_field(table_def, fld)
                if kind == "index" and fld is not None
                else (_NUMBER if kind == "createdAt" else _TEXT)
                for kind, fld in sort_cols
            ]
            return _paginate_result(
                q.paginate, table_def, filtered, sort_cols, col_types, direction
            )

        if unique:
            if len(filtered) > 1:
                raise RtDbError(
                    ErrorCode.PRECONDITION_FAILED,
                    "unique query matched multiple documents",
                )
            return _merge_doc(filtered[0]) if filtered else None
        if first:
            return _merge_doc(filtered[0]) if filtered else None

        limit = q.take if q.take is not None else MAX_TAKE
        return [_merge_doc(row) for row in filtered[:limit]]

    def _execute_get_terminal(self, q: Query, eq: list[Any], has_range: bool) -> Any:
        """``get(id)`` terminal: point read by id, exclusive of every other clause.

        Lift of the former inline ``if q.get is not None:`` arm of
        :meth:`run_query`; mirrors ``ts-client``'s ``executeGetTerminal``. The
        ``unique``/``first``/``count`` locals of ``run_query`` are read here
        straight off ``q`` (``bool | None`` — identical truthiness to the
        ``bool(q.*)`` precomputed locals).
        """
        if (
            q.index is not None
            or eq
            or has_range
            or q.order is not None
            or q.take is not None
            or q.unique
            or q.first
            or q.count
            or q.distinct
            or q.aggregate is not None
            or q.paginate is not None
            or q.filter is not None
            or q.search is not None
            or q.vector_search is not None
            or q.hybrid_search is not None
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "get cannot be combined with index, eq, range bounds, order, take, "
                "unique, first, count, distinct, aggregate, paginate, filter, search, "
                "vector search, or hybrid search",
            )
        assert q.get is not None  # caller dispatches only when get is set
        return self.get(q.table, q.get)

    def _execute_vector_search_terminal(
        self, q: Query, table_def: TableDef, eq: list[Any], has_range: bool
    ) -> list[dict[str, Any]]:
        """``vectorSearch`` terminal.

        Lift of the former inline ``if q.vector_search is not None:`` arm of
        :meth:`run_query`; mirrors ``ts-client``'s ``executeVectorSearchTerminal``.
        Vector similarity is not modeled in-memory, so every table row is a
        candidate (the sound over-approximation); a declared ``filter`` narrows
        the set via :func:`_eval_filter_expr`. The terminal's ``limit`` is not
        applied: without ranking there is no meaningful "top N".
        """
        assert q.vector_search is not None  # caller dispatches only when set
        if (
            q.index is not None
            or eq
            or has_range
            or q.order is not None
            or q.unique
            or q.first
            or q.count
            or q.filter is not None
            or q.search is not None
            or q.take is not None
            or q.paginate is not None
            or q.hybrid_search is not None
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "vectorSearch cannot be combined with any other terminal",
            )
        if q.vector_search.filter is not None:
            _validate_filter(q.vector_search.filter, set(table_def.fields.keys()))
        vector_candidates: list[StoredRow] = [
            row for (t, _id), row in self._docs.items() if t == q.table
        ]
        if q.vector_search.filter is not None:
            vector_candidates = [
                row
                for row in vector_candidates
                if _eval_filter_expr(q.vector_search.filter, row.doc)
            ]
        return [_merge_doc(row) for row in vector_candidates]

    def run(self, q: Query, model: type = dict) -> Any:
        """Typed wrapper around :meth:`run_query` that deserializes the result
        via :func:`par_rt_db.query.parse_result`. Pick ``model`` to match the
        terminal: ``list`` for ``take``/``collect`` (default ``dict`` per-doc),
        ``dict``/a Pydantic model for ``get``/``first``/``unique``, ``int`` for
        ``count``, ``Paginated`` shape for ``paginate``."""
        value = self.run_query(q)
        return parse_result(model, _terminal_of(q), value)

    # ---- mutate ---------------------------------------------------------

    def mutate(self, txn: Transaction, mut_id: str | None = None) -> list[StepResult]:
        """Execute a transaction and return one :class:`StepResult` per step, in
        order. A ``mut_id`` seen before short-circuits with the cached results
        (idempotency). On any step error the whole txn rolls back atomically and
        reactive subscriptions see nothing."""
        if mut_id is not None:
            cached = self._idempotency.get(mut_id)
            if cached is not None:
                return cached
        results = self._execute_transaction(txn)
        if mut_id is not None:
            self._idempotency[mut_id] = list(results)
        return results

    def _execute_transaction(self, txn: Transaction) -> list[StepResult]:
        if len(txn.steps) > MAX_STEPS:
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
        snapshot = dict(
            self._docs
        )  # shallow copy; StoredRow values are replaced, not mutated in place
        results: list[StepResult] = []
        write_set: set[str] = set()
        for step in txn.steps:
            try:
                result, written_table = self._execute_step(step)
            except RtDbError:
                # Atomicity: any step's error rolls back everything already applied.
                self._docs = snapshot
                raise
            results.append(result)
            if written_table is not None:
                write_set.add(written_table)
        self._notify_subs(write_set)
        return results

    def _execute_step(self, step: Step) -> tuple[StepResult, str | None]:
        match step:
            case _Insert(table=table, doc=doc):
                table_def = self._require_table(table)
                new_id = self._do_insert(table, table_def, doc)
                return _insert_result(new_id), table
            case _Patch(table=table, id=sid, fields=fields):
                table_def = self._require_table(table)
                self._do_patch(table_def, table, sid, fields)
                return None, table
            case _Replace(table=table, id=sid, doc=doc):
                table_def = self._require_table(table)
                self._do_replace(table_def, table, sid, doc)
                return None, table
            case _Delete(table=table, id=sid):
                self._require_table(table)
                self._do_delete(table, sid)
                return None, table
            case _ExpectVersion(table=table, id=sid, version=version):
                self._require_table(table)
                self._do_expect_version(table, sid, version)
                return None, None
            case _ExpectAbsent(table=table, index=index, eq=eq_vals):
                table_def = self._require_table(table)
                rows = self._eq_lookup(table_def, table, index, eq_vals)
                if rows:
                    raise RtDbError(
                        ErrorCode.PRECONDITION_FAILED,
                        f"index '{index}' already has a matching document",
                    )
                return None, None
            case _Upsert(
                table=table,
                index=index,
                eq=eq_vals,
                insert=insert_doc,
                patch=patch_fields,
            ):
                table_def = self._require_table(table)
                rows = self._eq_lookup(table_def, table, index, eq_vals)
                if len(rows) > 1:
                    raise RtDbError(
                        ErrorCode.PRECONDITION_FAILED, "upsert matched multiple documents"
                    )
                if rows:
                    row = rows[0]
                    merged = apply_patch(table_def, row.doc, patch_fields)
                    self._do_update(table_def, table, row.id, merged)
                    return _upsert_result(row.id, False), table
                new_id = self._do_insert(table, table_def, insert_doc)
                return _upsert_result(new_id, True), table
            case _PatchByQuery(table=table, filter=flt, patch=patch_fields, limit=limit_opt):
                table_def = self._require_table(table)
                _validate_filter(flt, set(table_def.fields.keys()))
                matched = [
                    row
                    for (t, _id), row in self._docs.items()
                    if t == table and _eval_filter_expr(flt, row.doc)
                ]
                matched.sort(key=lambda r: (r.created_at, r.id))
                limit = (
                    MAX_BY_QUERY_ROWS if limit_opt is None else min(limit_opt, MAX_BY_QUERY_ROWS)
                )
                truncated = len(matched) > limit
                take = matched[:limit]
                for row in take:
                    merged = apply_patch(table_def, row.doc, patch_fields)
                    self._do_update(table_def, table, row.id, merged)
                return _patch_by_query_result(len(take), truncated), table
            case _DeleteByQuery(table=table, filter=flt, limit=limit_opt):
                table_def = self._require_table(table)
                _validate_filter(flt, set(table_def.fields.keys()))
                matched = [
                    row
                    for (t, _id), row in self._docs.items()
                    if t == table and _eval_filter_expr(flt, row.doc)
                ]
                matched.sort(key=lambda r: (r.created_at, r.id))
                limit = (
                    MAX_BY_QUERY_ROWS if limit_opt is None else min(limit_opt, MAX_BY_QUERY_ROWS)
                )
                truncated = len(matched) > limit
                take = matched[:limit]
                for row in take:
                    self._do_delete(table, row.id)
                return _delete_by_query_result(len(take), truncated), table
            case _:
                raise RtDbError(ErrorCode.INTERNAL, "unknown step op")

    def _do_insert(self, table_name: str, table_def: TableDef, doc: dict[str, Any]) -> str:
        # TTL default: stamp the declared field at insert only when the caller
        # omitted it and a default duration is declared (mirrors server
        # `committer::execute_txn`). After insert the field is ordinary —
        # patch/replace/delete treat it like any other field. Runs before
        # validation so a required TTL field is populated.
        ttl = table_def.ttl
        if ttl is not None and ttl.default_duration_ms is not None and ttl.field not in doc:
            doc[ttl.field] = self._now() + ttl.default_duration_ms
        validate_doc(table_def, doc)
        stored = _strip_unset_optionals(table_def, doc)
        self._check_unique_indexes(table_def, table_name, stored, None)
        new_id = self._new_id()
        self._docs[(table_name, new_id)] = StoredRow(
            id=new_id, doc=stored, version=1, created_at=self._now()
        )
        return new_id

    def _do_patch(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        fields: dict[str, Any],
    ) -> None:
        key = (table_name, sid)
        row = self._docs.get(key)
        if row is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
        merged = apply_patch(table_def, row.doc, fields)
        self._do_update(table_def, table_name, sid, merged)

    def _do_replace(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        doc: dict[str, Any],
    ) -> None:
        key = (table_name, sid)
        row = self._docs.get(key)
        if row is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
        validate_doc(table_def, doc)
        stored = _strip_unset_optionals(table_def, doc)
        self._check_unique_indexes(table_def, table_name, stored, sid)
        row.doc = stored
        row.version += 1

    def _do_delete(self, table_name: str, sid: str) -> None:
        key = (table_name, sid)
        if self._docs.pop(key, None) is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")

    def _do_expect_version(self, table_name: str, sid: str, expected: int) -> None:
        row = self._docs.get((table_name, sid))
        if row is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
        if row.version != expected:
            raise RtDbError(
                ErrorCode.PRECONDITION_FAILED,
                f"version mismatch: expected {expected}, actual {row.version}",
            )

    def _do_update(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        merged: dict[str, Any],
    ) -> None:
        row = self._docs.get((table_name, sid))
        if row is not None:
            self._check_unique_indexes(table_def, table_name, merged, sid)
            row.doc = merged
            row.version += 1

    def _check_unique_indexes(
        self,
        table_def: TableDef,
        table_name: str,
        candidate_doc: dict[str, Any],
        exclude_id: str | None,
    ) -> None:
        """Enforce ``unique`` indexes on a candidate write (mirrors server
        ``CREATE UNIQUE INDEX`` and the TS/Rust ``checkUniqueIndexes``): for each
        unique index on ``table_name``, no OTHER row (excluding ``exclude_id``
        when given) that satisfies the index's ``where`` predicate may share the
        candidate's key values on the index's declared ``fields``. NULL/absent
        key fields disable the constraint for that row (Postgres ``UNIQUE`` treats
        NULLs as distinct). Raises ``CONFLICT`` on collision;
        :meth:`_execute_transaction` then rolls back the whole txn via the same
        snapshot/restore path as the ``PRECONDITION_FAILED`` checks. Uniqueness is
        on ``fields`` only — never ``id`` or ``created_at`` (a trailing
        tiebreaker column would defeat uniqueness, as it does on the server)."""
        for index in table_def.indexes:
            if not index.unique:
                continue
            pred = index.where
            # A partial unique index constrains only rows matching its predicate.
            if pred is not None and not _eval_filter_expr(pred, candidate_doc):
                continue
            # Build the collision key from declared `fields` only. NULL/absent key
            # fields disable the constraint for this row (Postgres UNIQUE treats
            # NULLs as distinct) — skip the index for this candidate.
            candidate_key = _collect_index_key(index.fields, candidate_doc)
            if candidate_key is None:
                continue
            for (t, _row_id), row in self._docs.items():
                if t != table_name:
                    continue
                if exclude_id is not None and row.id == exclude_id:
                    continue
                if pred is not None and not _eval_filter_expr(pred, row.doc):
                    continue
                row_key = _collect_index_key(index.fields, row.doc)
                if row_key is None:
                    continue
                if row_key == candidate_key:
                    raise RtDbError(
                        ErrorCode.CONFLICT,
                        f"unique index '{index.name}' violated",
                    )

    def _eq_lookup(
        self,
        table_def: TableDef,
        table_name: str,
        index_name: str,
        eq: list[Any],
    ) -> list[StoredRow]:
        index = _require_index(table_def, index_name)
        if len(eq) != len(index.fields):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"index '{index_name}' expects {len(index.fields)} eq value(s), got {len(eq)}",
            )
        typed = [
            _coerce_index_value(table_def, fld, value)
            for fld, value in zip(index.fields, eq, strict=True)
        ]
        matches: list[StoredRow] = []
        for (t, _id), row in self._docs.items():
            if t != table_name:
                continue
            if all(
                (rv := row.doc.get(fld)) is not None and rv == tv
                for fld, tv in zip(index.fields, typed, strict=True)
            ):
                matches.append(row)
        return matches

    def _require_table(self, name: str) -> TableDef:
        def_ = self._tables.get(name)
        if def_ is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"table '{name}' not found")
        return def_

    def _new_id(self) -> str:
        ts = self._now() & ((1 << 48) - 1)
        rand = self._random_hex(19)
        return f"{ts:012x}7{rand}"

    def _random_hex(self, count: int) -> str:
        digits = "0123456789abcdef"
        return "".join(digits[int(self._random() * 16) & 0xF] for _ in range(count))

    # ---- subscriptions --------------------------------------------------

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
        sub.last = _canonical(initial)
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
            nxt_canon = _canonical(nxt)
            if sub.last is None or sub.last != nxt_canon:
                sub.last = nxt_canon
                fires.append((sub.callback, nxt))
        # Lazily compact dead subscriptions.
        self._subscribers = [s for s in self._subscribers if s.alive[0]]
        for callback, value in fires:
            callback(value)

    # ---- presence (ENH-015) --------------------------------------------
    #
    # Ports ``presence``/``updatePresence``/``leavePresence``
    # (``ts-client/src/in_memory.ts``, ``rust-client/src/in_memory.rs``). Backed
    # by :class:`PresenceRooms`, which approximates the server's per-db presence
    # registry: one client = one connection, keyed by ``connectionId``. Two
    # clients sharing the same ``PresenceRooms`` see each other.

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

    # ---- schedules ------------------------------------------------------

    def schedule(self, txn: Transaction, when: ScheduleWhen) -> str:
        """Store ``txn`` scheduled for ``when`` and return its id. Cron
        validation is deferred to the live server; the harness accepts any
        expression."""
        new_id = self._new_id()
        now = self._now()
        match when:
            case Cron(expr=expr_str):
                kind = "cron"
                cron: str | None = expr_str
            case _:
                kind = "oneshot"
                cron = None
        self._schedules.append(
            _ScheduledJob(
                id=new_id,
                kind=kind,
                txn=txn,
                due_at=self._due_at_for(when, now),
                cron=cron,
                status="pending",
                created_at=now,
                fired_count=0,
                last_error=None,
            )
        )
        return new_id

    def cancel_schedule(self, id: str) -> None:
        """Remove the scheduled job. :class:`RtDbError` ``NOT_FOUND`` if no such id."""
        before = len(self._schedules)
        self._schedules = [j for j in self._schedules if j.id != id]
        if len(self._schedules) == before:
            raise RtDbError(ErrorCode.NOT_FOUND, f"schedule '{id}' not found")

    def pause_schedule(self, id: str) -> None:
        """Set the schedule's status to ``paused``. ``NOT_FOUND`` if no such id."""
        job = self._find_job(id)
        if job is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"schedule '{id}' not found")
        job.status = "paused"

    def resume_schedule(self, id: str) -> None:
        """Set a paused schedule's status back to ``pending``. ``NOT_FOUND`` if no such id."""
        job = self._find_job(id)
        if job is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"schedule '{id}' not found")
        job.status = "pending"

    def list_schedules(self) -> list[ScheduleInfo]:
        """Snapshot of every scheduled job's public view."""
        return [_schedule_info(job) for job in self._schedules]

    def _reap_ttl(self, now: int) -> int:
        """Remove docs whose declared TTL ``field`` (a number) is ``< now`` — the
        in-memory mirror of the server's per-tick TTL reaper. Fires only on
        tables that declare ``ttl``; non-numeric or absent values are left alone.
        Notifies subscribers on each touched table so reactive subscriptions see
        the expiry as a delete. Returns the count of removed docs."""
        touched: set[str] = set()
        removed = 0
        # Snapshot the items — popping mid-iteration would skip rows.
        for (table, doc_id), row in list(self._docs.items()):
            tdef = self._tables.get(table)
            if tdef is None or tdef.ttl is None:
                continue
            value = row.doc.get(tdef.ttl.field)
            if isinstance(value, (int, float)) and value < now:
                self._docs.pop((table, doc_id), None)
                removed += 1
                touched.add(table)
        if touched:
            self._notify_subs(touched)
        return removed

    def tick(self, now_ms: int | None = None) -> None:
        """Advance the harness clock to ``now_ms`` (or the client clock when
        omitted), then (1) reap docs whose TTL field is in the past and (2) fire
        every due non-paused scheduled job by applying its txn through the same
        atomic path as :meth:`mutate` (so reactive subscriptions see the write).
        One-shots are removed after a successful fire; crons re-arm by
        :data:`CRON_STEP_MS`. A job whose txn fails is marked ``error`` but left
        in place (still due), so a subsequent ``tick`` retries it."""
        now = now_ms if now_ms is not None else self._now()
        self._reap_ttl(now)
        i = 0
        while i < len(self._schedules):
            job = self._schedules[i]
            if job.status == "paused" or job.due_at > now:
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
            else:
                j = self._find_job(job_id)
                if j is not None:
                    j.fired_count += 1
                    if kind == "oneshot":
                        # Remove after a successful fire; don't bump i (the next
                        # job shifts into this index).
                        self._schedules = [s for s in self._schedules if s.id != job_id]
                        continue
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
            case _:
                return now + CRON_STEP_MS  # cron

    # ---- file storage ---------------------------------------------------

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


# ---------------------------------------------------------------------------
# Result constructors (route through the StepResult union adapter so we don't
# couple to the private result-variant classes).
# ---------------------------------------------------------------------------


def _insert_result(id: str) -> StepResult:
    return _STEP_RESULT.validate_python({"id": id})


def _upsert_result(id: str, inserted: bool) -> StepResult:
    return _STEP_RESULT.validate_python({"id": id, "inserted": inserted})


def _patch_by_query_result(patched: int, truncated: bool) -> StepResult:
    return _STEP_RESULT.validate_python({"patched": patched, "truncated": truncated})


def _delete_by_query_result(deleted: int, truncated: bool) -> StepResult:
    return _STEP_RESULT.validate_python({"deleted": deleted, "truncated": truncated})


# ---------------------------------------------------------------------------
# Free helpers — ports of the module-private functions in the Rust/TS harness.
# ---------------------------------------------------------------------------


def _canonical(value: Any) -> str:
    """Canonical string form for change detection, independent of key order."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


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


def apply_patch(table: TableDef, doc: dict[str, Any], fields: dict[str, Any]) -> dict[str, Any]:
    """Apply a patch's ``fields`` onto ``doc``. A ``None`` onto an ``Optional``
    field whose inner type doesn't itself accept ``None`` deletes the key; the
    merged doc is then re-validated whole. Raises ``SCHEMA_VIOLATION`` on violation."""
    merged = dict(doc)
    for fld, value in fields.items():
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
    validate_doc(table, merged)
    return merged


def _migrate_table_mut(schema: SchemaDef, table: str) -> TableDef:
    """Return a mutable reference to ``table`` within ``schema``, or raise
    ``BAD_REQUEST`` if the table does not exist. Mirrors the server's
    ``migrate::table_mut``."""
    t = schema.tables.get(table)
    if t is None:
        raise RtDbError(ErrorCode.BAD_REQUEST, f"table '{table}' does not exist")
    return t


def _cast_valid_for(cast: Cast, old_ty: Any) -> bool:
    """Mirror of ``server::migrate::cast_valid_for`` — true if ``cast`` can
    coerce from the ``old_ty`` field type."""
    if cast == Cast.TO_STRING:
        return isinstance(old_ty, (_FString, _FNumber, _FBoolean, _FInt64))
    if cast == Cast.TO_NUMBER:
        return isinstance(old_ty, (_FString, _FBoolean, _FInt64))
    if cast == Cast.TO_INT64:
        return isinstance(old_ty, (_FString, _FNumber))
    if cast == Cast.TO_BOOLEAN:
        return isinstance(old_ty, (_FString, _FNumber))
    return False


def _coerce_value(cast: Cast, v: Any) -> Any:
    """Pure-Python coercion mirroring ``server::migrate::coerce_value`` (and
    ``rust-client::in_memory::coerce_value``). Returns the coerced value or
    ``None`` if the value cannot be coerced under this cast.

    ``ToInt64`` emits a decimal-string (int64 travels as a canonical decimal
    string on the wire — see ``schema::is_valid_int64`` and
    ``FEATURE_MATRIX.md`` #13); ``ToNumber`` emits a ``float``; the others
    produce the natural Python representation.
    """
    if cast == Cast.TO_STRING:
        if isinstance(v, str):
            return v
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return None
    if cast == Cast.TO_NUMBER:
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                f = float(v)
            except ValueError:
                return None
            if not math.isfinite(f):
                return None
            return f
        return None
    if cast == Cast.TO_INT64:
        # ``bool`` is a subclass of ``int`` — check it first and reject (mirrors
        # Rust's ``Value::Bool`` falling through to ``_ => None``).
        if isinstance(v, bool):
            return None
        # i64 range: the server's ``i64::from_str`` / ``Number::as_i64`` reject
        # outside [-(2**63), 2**63), but Python ints are arbitrary-precision, so
        # bound explicitly to keep parity (else a huge int silently "coerces").
        i64_min, i64_max = -(2**63), 2**63 - 1
        if isinstance(v, int):
            if not i64_min <= v <= i64_max:
                return None
            return str(v)
        if isinstance(v, float):
            if not v.is_integer():
                return None
            iv = int(v)
            if not i64_min <= iv <= i64_max:
                return None
            return str(iv)
        if isinstance(v, str):
            try:
                iv = int(v)
            except ValueError:
                return None
            if not i64_min <= iv <= i64_max:
                return None
            return str(iv)
        return None
    if cast == Cast.TO_BOOLEAN:
        if isinstance(v, str):
            if v in ("true", "1"):
                return True
            if v in ("false", "0"):
                return False
            return None
        # ``bool`` is a subclass of ``int`` — check it first and reject
        # (``ToBoolean`` only accepts String and Number source types).
        if isinstance(v, bool):
            return None
        if isinstance(v, (int, float)):
            return v != 0.0
        return None
    return None


def _detect_destructive_changes(old: SchemaDef, new: SchemaDef) -> None:
    """Mirror of ``server/src/ddl.rs::detect_destructive_changes``: reject any
    removed table, removed/changed field, or removed/changed index with
    ``BAD_REQUEST``. Additive changes (new tables/fields/indexes) pass through."""
    for table_name, old_table in old.tables.items():
        new_table = new.tables.get(table_name)
        if new_table is None:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"removed table '{table_name}'")
        for field_name, old_field_type in old_table.fields.items():
            new_field_type = new_table.fields.get(field_name)
            if new_field_type is None:
                raise RtDbError(ErrorCode.BAD_REQUEST, f"removed field '{table_name}.{field_name}'")
            if _field_type_signature(old_field_type) != _field_type_signature(
                new_field_type
            ) and not _is_widening_of(old_field_type, new_field_type):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed type of field '{table_name}.{field_name}'",
                )
        for old_index in old_table.indexes:
            new_index = next((i for i in new_table.indexes if i.name == old_index.name), None)
            if new_index is None:
                raise RtDbError(ErrorCode.BAD_REQUEST, f"removed index '{old_index.name}'")
            if new_index.fields != old_index.fields:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed fields of index '{old_index.name}'",
                )
            if bool(new_index.search) != bool(old_index.search):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed kind of index '{old_index.name}' (btree <-> search)",
                )
            if _vector_signature(new_index.vector) != _vector_signature(old_index.vector):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed vector spec of index '{old_index.name}'",
                )
            if bool(new_index.unique) != bool(old_index.unique):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed uniqueness of index '{old_index.name}'",
                )
            if _where_signature(new_index.where) != _where_signature(old_index.where):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed partial predicate of index '{old_index.name}'",
                )


def _field_type_signature(ty: Any) -> Any:
    """Structural signature of a ``FieldType`` for destructive-change detection
    (the live server compares the parsed type tree directly)."""
    return ty.model_dump(mode="json")


def _literal_set(ty: Any) -> list[Any] | None:
    """Finite literal set carried by a ``_FLiteral`` or a ``_FUnion`` of pure
    literals — a port of ``server/src/schema.rs::literal_set``. Returns ``None``
    for anything that is not a finite literal set (scalars, optionals, objects,
    arrays, mixed or empty unions)."""
    match ty:
        case _FLiteral(value=v):
            return [v]
        case _FUnion(variants=variants):
            if not variants:
                return None
            out: list[Any] = []
            for variant in variants:
                match variant:
                    case _FLiteral(value=v):
                        out.append(v)
                    case _:
                        return None
            return out
        case _:
            return None


def _is_widening_of(old: Any, new: Any) -> bool:
    """``True`` iff ``new`` carries a finite literal set that is a superset of
    ``old``'s — a port of ``server/src/schema.rs::is_widening_of``. Lets
    ``pushSchema`` accept additive widening of a literal-union field (e.g.
    ``{a,b}`` -> ``{a,b,c}``, or ``"a"`` -> ``{a,b}``) as a non-destructive
    change."""
    old_vals = _literal_set(old)
    new_vals = _literal_set(new)
    if old_vals is None or new_vals is None:
        return False
    return all(any(o == n for n in new_vals) for o in old_vals)


def _vector_signature(spec: Any) -> Any:
    if spec is None:
        return None
    return spec.model_dump(mode="json")


def _where_signature(pred: Any) -> Any:
    """Structural signature of an ``IndexDef.where`` predicate (a
    ``FilterExpr``) for destructive-change detection — the live server compares
    the parsed ``FilterExpr`` tree directly."""
    if pred is None:
        return None
    return pred.model_dump(mode="json")


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


def _is_number(value: Any) -> bool:
    """``True`` iff ``value`` is a JSON number (booleans excluded)."""
    return isinstance(value, float | int) and not isinstance(value, bool)


def _apply_aggregate(op: str, values: list[Any], pg: _PgType) -> Any:
    """Apply one aggregate op over a non-empty list of non-null values, mirroring
    the server's SQL semantics and the TS/Rust harnesses. SUM/AVG reduce
    numerically (int64 values are decimal strings -> parsed); MIN/MAX pick the
    smallest/largest per :func:`_compare_index_values`, so a string field's
    extremes match Postgres lexicographic ordering. Only called on non-empty
    input — the caller maps an empty set to ``None``."""
    if op in (AggregateOp.SUM, AggregateOp.AVG):
        nums = [_to_numeric(v, pg) for v in values]
        total = sum(nums)
        return total / len(values) if op == AggregateOp.AVG else total
    want_min = op == AggregateOp.MIN
    best = values[0]
    for v in values[1:]:
        c = _compare_index_values(v, best, pg)
        if c < 0 if want_min else c > 0:
            best = v
    return best


def _to_numeric(v: Any, pg: _PgType) -> float | int:
    """Reduce one index value to a number for SUM/AVG. ``_INT64`` values are
    decimal strings on the wire -> parsed to int; ``_NUMBER`` values are floats."""
    if pg == _INT64:
        return _parse_i64(v)
    return _to_float(v)


def _dedupe_key(v: Any) -> str:
    """Canonical JSON key so equal scalars (and equal compound values) share a
    key. Distinct/group keys are always index fields (scalars), so this reduces
    to the scalar's string form in practice."""
    return json.dumps(v, sort_keys=True, separators=(",", ":"))


def _dir_order(o: int, direction: str) -> int:
    return o if direction == "asc" else -o


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
            "status": job.status,
            "lastError": job.last_error,
            "createdAt": job.created_at,
            "firedCount": job.fired_count,
        }
    )


# ---------------------------------------------------------------------------
# Keyset-cursor pagination
# ---------------------------------------------------------------------------


def _paginate_result(
    paginate: Any,
    table_def: TableDef,
    sorted_rows: list[StoredRow],
    sort_cols: list[tuple[str, str | None]],
    col_types: list[_PgType],
    direction: str,
) -> dict[str, Any]:
    num_items = min(int(paginate.num_items), MAX_TAKE)
    cursor_values: list[Any] | None = None
    if paginate.cursor is not None:
        try:
            decoded = decode_cursor(paginate.cursor)
        except ValueError as err:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"invalid cursor: {err}") from err
        if len(decoded) != len(sort_cols):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"cursor has {len(decoded)} value(s) but this query sorts over "
                f"{len(sort_cols)} column(s)",
            )
        _validate_cursor_values(decoded, sort_cols, table_def)
        cursor_values = decoded

    if cursor_values is not None:
        rows = [
            row
            for row in sorted_rows
            if _is_after_cursor(row, cursor_values, sort_cols, col_types, direction)
        ]
    else:
        rows = sorted_rows

    has_next = len(rows) > num_items
    page = rows[:num_items]
    docs = [_merge_doc(row) for row in page]

    out: dict[str, Any] = {"docs": docs}
    if has_next and page:
        last = page[-1]
        keyset = [_sort_value(last, col) for col in sort_cols]
        out["nextCursor"] = encode_cursor(keyset)
    return out


def _validate_cursor_values(
    cursor_values: list[Any],
    sort_cols: list[tuple[str, str | None]],
    table_def: TableDef,
) -> None:
    for (kind, fld), value in zip(sort_cols, cursor_values, strict=True):
        if kind == "index":
            if fld is not None and value is not None:
                _coerce_index_value(table_def, fld, value)
        elif kind == "createdAt" and not _is_number(value):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "cursor value for created_at must be a number",
            )
        elif kind == "id" and not isinstance(value, str):
            raise RtDbError(ErrorCode.BAD_REQUEST, "cursor value for id must be a string")


def _is_after_cursor(
    row: StoredRow,
    cursor_values: list[Any],
    sort_cols: list[tuple[str, str | None]],
    col_types: list[_PgType],
    direction: str,
) -> bool:
    for i in range(len(sort_cols)):
        prefix_equal = True
        for j in range(i):
            rv = _sort_value(row, sort_cols[j])
            if _compare_index_values(rv, cursor_values[j], col_types[j]) != 0:
                prefix_equal = False
                break
        if not prefix_equal:
            continue
        rv = _sort_value(row, sort_cols[i])
        c = _compare_index_values(rv, cursor_values[i], col_types[i])
        ahead = c > 0 if direction == "asc" else c < 0
        if ahead:
            return True
    return False


def _sort_value(row: StoredRow, col: tuple[str, str | None]) -> Any:
    kind, fld = col
    if kind == "createdAt":
        return row.created_at
    if kind == "id":
        return row.id
    return row.doc.get(fld) if fld is not None else None


# ---------------------------------------------------------------------------
# Filter evaluation
# ---------------------------------------------------------------------------


def _validate_filter(expr: FilterExpr, fields: set[str]) -> None:
    """Structural validation of a ``FilterExpr`` against a table's declared
    fields. Raises ``BAD_REQUEST`` for an empty ``and``/``or``/``in``, an unknown
    field, a non-string/number/boolean leaf value, or mixed-type ``in`` values."""
    match expr:
        case _FilterAnd(exprs=exprs) | _FilterOr(exprs=exprs):
            if not exprs:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST, f"{expr.op} filter requires at least one expr"
                )
            for e in exprs:
                _validate_filter(e, fields)
        case _FilterIn(field=fld, values=values):
            if not values:
                raise RtDbError(ErrorCode.BAD_REQUEST, "in filter requires at least one value")
            for v in values:
                _check_leaf_value(fld, v, fields)
            first_kind = _in_value_kind(values[0])
            for v in values[1:]:
                if _in_value_kind(v) != first_kind:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        "in filter values must all be the same type",
                    )
        case (
            _FilterEq(field=fld, value=val)
            | _FilterNeq(field=fld, value=val)
            | _FilterGt(field=fld, value=val)
            | _FilterGte(field=fld, value=val)
            | _FilterLt(field=fld, value=val)
            | _FilterLte(field=fld, value=val)
        ):
            _check_leaf_value(fld, val, fields)
        case _FilterNot(expr=inner):
            _validate_filter(inner, fields)
        case _FilterContains(field=fld, value=val):
            _check_leaf_value(fld, val, fields)
        case _FilterExists(field=fld):
            if fld not in fields:
                raise RtDbError(ErrorCode.BAD_REQUEST, f"filter references unknown field '{fld}'")
        case _:
            raise RtDbError(ErrorCode.INTERNAL, "unknown filter op")


def _check_leaf_value(field: str, value: Any, fields: set[str]) -> None:
    if field not in fields:
        raise RtDbError(ErrorCode.BAD_REQUEST, f"filter references unknown field '{field}'")
    if isinstance(value, bool):
        return
    if isinstance(value, str | float | int):
        return
    raise RtDbError(ErrorCode.BAD_REQUEST, "filter value must be a string, number, or boolean")


def _in_value_kind(value: Any) -> str:
    if isinstance(value, str):
        return "string"
    if isinstance(value, float | int) and not isinstance(value, bool):
        return "number"
    return "boolean"


def _eval_filter_expr(expr: FilterExpr, doc: dict[str, Any]) -> bool:
    """Evaluate a ``FilterExpr`` predicate against a stored doc. A null/absent
    field never matches (SQL NULL exclusion). Assumes ``_validate_filter`` passed."""
    match expr:
        case _FilterAnd(exprs=exprs):
            return all(_eval_filter_expr(e, doc) for e in exprs)
        case _FilterOr(exprs=exprs):
            return any(_eval_filter_expr(e, doc) for e in exprs)
        case _FilterIn(field=fld, values=values):
            return any(_compare_leaf("eq", fld, v, doc) for v in values)
        case (
            _FilterEq(field=fld, value=val)
            | _FilterNeq(field=fld, value=val)
            | _FilterGt(field=fld, value=val)
            | _FilterGte(field=fld, value=val)
            | _FilterLt(field=fld, value=val)
            | _FilterLte(field=fld, value=val)
        ):
            return _compare_leaf(expr.op, fld, val, doc)
        case _FilterNot(expr=inner):
            return not _eval_filter_expr(inner, doc)
        case _FilterContains(field=fld, value=val):
            arr = doc.get(fld)
            want = json.dumps(val, sort_keys=True)
            return isinstance(arr, list) and any(json.dumps(v, sort_keys=True) == want for v in arr)
        case _FilterExists(field=fld):
            return doc.get(fld) is not None
        case _:
            return False


def _compare_leaf(op: str, field: str, filter_value: Any, doc: dict[str, Any]) -> bool:
    doc_val = doc.get(field)
    if doc_val is None:
        return False
    if isinstance(filter_value, str):
        return _compare_values(op, _doc_to_text(doc_val), filter_value)
    if isinstance(filter_value, bool):
        return isinstance(doc_val, bool) and _compare_values(op, doc_val, filter_value)
    if isinstance(filter_value, float | int):
        lhs = _doc_to_number(doc_val)
        if lhs is None:
            return False
        return _compare_values(op, lhs, float(filter_value))
    return False


def _doc_to_text(doc_val: Any) -> str:
    """Mirrors Postgres ``doc->>'field'``: the JSON text of the value."""
    if isinstance(doc_val, bool):
        return "true" if doc_val else "false"
    if isinstance(doc_val, int):
        return str(doc_val)
    if isinstance(doc_val, float):
        if math.isfinite(doc_val) and doc_val == int(doc_val) and abs(doc_val) <= 2**53:
            return str(int(doc_val))
        return json.dumps(doc_val)
    if isinstance(doc_val, str):
        return doc_val
    return json.dumps(doc_val)


def _doc_to_number(doc_val: Any) -> float | None:
    """Mirrors Postgres ``(doc->>'field')::float8``: a finite number, or a parsed
    numeric string."""
    if isinstance(doc_val, bool):
        return None
    if isinstance(doc_val, float | int):
        f = float(doc_val)
        return f if math.isfinite(f) else None
    if isinstance(doc_val, str):
        s = doc_val.strip()
        if not s:
            return None
        try:
            f = float(s)
        except ValueError:
            return None
        return f if math.isfinite(f) else None
    return None


def _compare_values(op: str, lhs: Any, rhs: Any) -> bool:
    if op == "eq":
        return lhs == rhs
    if op == "neq":
        return lhs != rhs
    if op == "gt":
        return lhs > rhs
    if op == "gte":
        return lhs >= rhs
    if op == "lt":
        return lhs < rhs
    if op == "lte":
        return lhs <= rhs
    return False


# Suppress an unused-import warning for `field` (re-exported for parity with the
# Rust harness's public surface; not used internally).
__all__ = [
    "CRON_STEP_MS",
    "FileMetadata",
    "InMemoryRtDbClient",
    "InMemoryRtDbClientOptions",
    "MAX_STEPS",
    "MAX_TAKE",
    "StoredBlob",
    "StoredRow",
    "SubscriptionHandle",
    "UploadResult",
    "apply_patch",
    "is_base64_string",
    "is_hex_id",
    "is_int64_string",
    "validate_doc",
    "validate_value",
]
