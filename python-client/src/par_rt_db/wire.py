"""Wire types — the fourth implementation of par-rt-db's JSON contract.

Mirrors ``server/src/protocol.rs`` and ``server/src/query.rs`` (cross-checked
against ``ts-client/src/protocol.ts`` and ``rust-client/src/wire.rs``) field-for-field.
Discriminator unions:

* ``ScheduleWhen`` is tagged by ``type`` (camelCase variants:
  ``afterMs``/``runAt``/``cron``/``interval``).
* ``FilterExpr`` is tagged by ``op`` (lowercase variants: ``eq``/``neq``/``gt``/``gte``/
  ``lt``/``lte``/``in``/``and``/``or``/``not``/``contains``/``exists``/``olderThan``).

Message and Schema families (``ClientMessage``/``ServerMessage``) are appended in
later tasks; the leaf types below are placed so that append is clean.

``extra='forbid'`` everywhere mirrors Rust's ``deny_unknown_fields``. Message/Schema/
Schedule/AuthedUser fields are camelCase on the wire; Python names are snake_case
via ``alias_generator=to_camel``.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer
from pydantic_core.core_schema import SerializerFunctionWrapHandler


def to_camel(name: str) -> str:
    """snake_case -> camelCase alias (e.g. ``due_at`` -> ``dueAt``)."""
    head, *tail = name.split("_")
    return head + "".join(p.title() for p in tail)


class _Camel(BaseModel):
    """Base for wire models whose JSON keys are camelCase and reject unknown fields."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )


#: The wire protocol version this client speaks (ARC-013). Sent as
#: ``protocolVersion`` on the WS ``auth`` frame and as the ``X-Rtdb-Protocol``
#: HTTP header; a server whose ``PROTOCOL_VERSION`` is older rejects a value
#: greater than its own with ``UNSUPPORTED_PROTOCOL``. Mirrors server
#: ``protocol::PROTOCOL_VERSION``.
PROTOCOL_VERSION = 1


class AuthedUser(_Camel):
    """Authenticated principal.

    ``email``/``name`` serialize as JSON ``null`` when absent; ``githubLogin``/
    ``githubId`` are omitted entirely on the wire when ``None`` (mirrors the
    server's ``#[serde(skip_serializing_if = "Option::is_none")]``).

    ``kind`` is narrowed to ``Literal["user", "machine"]`` (ARC-004/QA-008) so a
    typo is rejected at parse time, mirroring the server's ``UserKind`` enum.
    """

    kind: Literal["user", "machine"]
    email: str | None = None
    name: str | None = None
    github_login: str | None = None
    github_id: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_github(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        for alias in ("githubLogin", "githubId"):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


# --- ScheduleWhen (discriminator "type", camelCase) ---


class AfterMs(_Camel):
    """Fire once, ``ms`` milliseconds from now.

    Attributes:
        ms: Delay in milliseconds from the scheduling moment.
    """

    type: Literal["afterMs"] = "afterMs"
    ms: int


class RunAt(_Camel):
    """Fire once, at a specific UTC epoch-ms instant.

    A timestamp already in the past fires immediately.

    Attributes:
        ms: Target UTC epoch-ms instant.
    """

    type: Literal["runAt"] = "runAt"
    ms: int


class Cron(_Camel):
    """Fire repeatedly on a 5-field cron schedule (UTC, minute-first).

    Attributes:
        expr: The cron expression.
    """

    type: Literal["cron"] = "cron"
    expr: str


class Interval(_Camel):
    """Fire repeatedly, every ``every_ms`` milliseconds.

    The first fire is one interval from now. Missed windows (downtime,
    pause) are skipped, never backfilled — each fire re-arms from its
    actual fire time, like cron recompute.

    Attributes:
        every_ms: Fixed recurrence interval in milliseconds.
    """

    type: Literal["interval"] = "interval"
    every_ms: int


ScheduleWhen = Annotated[AfterMs | RunAt | Cron | Interval, Field(discriminator="type")]

# Backwards-compat aliases — the underscore spellings were the only way to
# reach these before ARC-109 re-exported them from the package root. Kept so an
# existing `from par_rt_db.wire import _AfterMs` keeps resolving.
_AfterMs = AfterMs
_RunAt = RunAt
_Cron = Cron


class ScheduleInfo(_Camel):
    """A scheduled job's public view (returned by ``listSchedules``).

    ``cron``/``everyMs``/``lastError`` are omitted on the wire when ``None``
    (only interval jobs carry ``everyMs`` — exactly like ``cron``). ``kind`` and
    ``status`` are narrowed to ``Literal`` unions (ARC-004/QA-008) mirroring the
    server's ``ScheduleKind`` / ``ScheduleStatus`` enums.
    """

    id: str
    kind: Literal["oneshot", "cron", "interval"]
    due_at: int
    cron: str | None = None
    every_ms: int | None = None
    status: Literal["pending", "running", "paused", "error"]
    last_error: str | None = None
    created_at: int
    fired_count: int

    @model_serializer(mode="wrap")
    def _drop_none_optional(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        for alias in ("cron", "everyMs", "lastError"):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


# --- Workflows (FM-29) ---


class StepRetry(_Camel):
    """Per-step retry policy. ``max_attempts`` counts TOTAL attempts — the
    first try included. Defaults when a step omits ``retry`` (server side):
    3 attempts, 1s initial backoff doubling to a 60s cap."""

    max_attempts: int
    initial_retry_ms: int = 1_000
    max_retry_ms: int = 60_000


class AwaitSignalSpec(_Camel):
    """An ``awaitSignal`` step's wait declaration: park the run until a signal
    named ``name`` is delivered; ``timeout_ms`` bounds each wait attempt
    (omitted = wait indefinitely, cancel is the escape). ``timeoutMs`` is
    omitted on the wire when ``None``."""

    name: str
    timeout_ms: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_optional(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("timeoutMs") is None:
            out.pop("timeoutMs", None)
        return out


class WorkflowStepSpec(_Camel):
    """One workflow step: either an ordinary ``Transaction`` or an
    :class:`AwaitSignalSpec` wait (exactly one — submit-time validation
    enforces it), plus policy. The txn may itself carry schedule/cancelSchedule
    steps. ``txn``/``await_signal``/``retry``/``sleep_before_ms`` are omitted on
    the wire when ``None``.

    ``txn`` is deliberately ``dict[str, Any]`` (the dumped ``Transaction``)
    rather than the ``Transaction`` model itself, keeping the wire layer
    decoupled from the DSL layer (avoids a circular import with
    ``mutation.py``) — same convention as ``_ClientSchedule.txn``."""

    txn: dict[str, Any] | None = None
    await_signal: AwaitSignalSpec | None = None
    retry: StepRetry | None = None
    sleep_before_ms: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_optional(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        for alias in ("txn", "awaitSignal", "retry", "sleepBeforeMs"):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


class WorkflowSpec(_Camel):
    """A submitted workflow definition. Stored verbatim per run — a run
    snapshots its spec, so template edits never drift a live run."""

    name: str
    steps: list[WorkflowStepSpec]


# Snake-case wire values (server enum is rename_all = "snake_case"). "waiting"
# is the parked-at-an-awaitSignal-step state (only a delivery, a timeout, or a
# cancel leaves it).
WorkflowStatus = Literal["pending", "running", "waiting", "success", "failed", "cancelled"]

# Lowercase wire values (server OutcomeStatus enum).
OutcomeStatus = Literal["success", "failed"]


class StepOutcome(_Camel):
    """Terminal record for one step: completed successfully, or exhausted its
    retries (``status: "failed"``). Individual retried attempts are NOT
    recorded — the ``attempts`` count carries them. ``error``/``signal`` are
    omitted on the wire when ``None``; ``signal`` carries a delivered
    awaitSignal payload verbatim."""

    step_index: int
    status: OutcomeStatus
    attempts: int
    at: int
    error: str | None = None
    signal: Any | None = None

    @model_serializer(mode="wrap")
    def _drop_none_optional(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        for alias in ("error", "signal"):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


class WorkflowInfo(_Camel):
    """List/get projection of one run. ``sleep_until``/``last_error``/
    ``waiting_for``/``waited_since``/``started_at``/``finished_at`` are omitted
    on the wire when ``None`` (the wait fields project only while the run is
    parked at an ``awaitSignal`` step)."""

    id: str
    name: str
    status: WorkflowStatus
    current_step: int
    step_count: int
    attempts: int
    sleep_until: int | None = None
    last_error: str | None = None
    waiting_for: str | None = None
    waited_since: int | None = None
    created_at: int
    updated_at: int
    started_at: int | None = None
    finished_at: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_optional(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        for alias in (
            "sleepUntil",
            "lastError",
            "waitingFor",
            "waitedSince",
            "startedAt",
            "finishedAt",
        ):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


class WorkflowInfoFull(WorkflowInfo):
    """The ``GET .../{id}`` shape: the info row (flattened on the wire — a
    pydantic subclass models ``#[serde(flatten)]``) plus the per-step outcome
    trail."""

    step_outcomes: list[StepOutcome]


# --- FilterExpr (discriminator "op", lowercase) ---


class _FilterLeaf(_Camel):
    field: str


class _FilterEq(_FilterLeaf):
    op: Literal["eq"] = "eq"
    value: Any


class _FilterNeq(_FilterLeaf):
    op: Literal["neq"] = "neq"
    value: Any


class _FilterGt(_FilterLeaf):
    op: Literal["gt"] = "gt"
    value: Any


class _FilterGte(_FilterLeaf):
    op: Literal["gte"] = "gte"
    value: Any


class _FilterLt(_FilterLeaf):
    op: Literal["lt"] = "lt"
    value: Any


class _FilterLte(_FilterLeaf):
    op: Literal["lte"] = "lte"
    value: Any


class _FilterIn(_FilterLeaf):
    op: Literal["in"] = "in"
    values: list[Any]


class _FilterAnd(_Camel):
    op: Literal["and"] = "and"
    exprs: list[FilterExpr]


class _FilterOr(_Camel):
    op: Literal["or"] = "or"
    exprs: list[FilterExpr]


class _FilterNot(_Camel):
    op: Literal["not"] = "not"
    expr: FilterExpr


class _FilterContains(_FilterLeaf):
    op: Literal["contains"] = "contains"
    value: Any


class _FilterExists(_FilterLeaf):
    op: Literal["exists"] = "exists"


class _FilterOlderThan(_FilterLeaf):
    """Execution-time-relative age predicate (by-query step filters only —
    ``patchByQuery``/``deleteByQuery``): the field's epoch-ms value is strictly
    older than ``now − ms`` with ``now`` read from the engine clock at each
    execution. Read/query filters, ``authorize`` predicates, partial-index
    ``where`` predicates, and computed ``case`` whens reject it (mirrors
    server ``dsl.rs::FilterExpr::OlderThan``)."""

    op: Literal["olderThan"] = "olderThan"
    ms: int


FilterExpr = Annotated[
    (
        _FilterEq
        | _FilterNeq
        | _FilterGt
        | _FilterGte
        | _FilterLt
        | _FilterLte
        | _FilterIn
        | _FilterAnd
        | _FilterOr
        | _FilterNot
        | _FilterContains
        | _FilterExists
        | _FilterOlderThan
    ),
    Field(discriminator="op"),
]


#: Match mode for the ``search`` terminal (FM-30): ``"tsquery"`` (the default —
#: today's full-text behavior, also the behavior when ``mode`` is omitted) or
#: ``"trgm"`` (case-insensitive substring/autocomplete matching over the
#: index's text fields). Mirrors ``server/src/query.rs::SearchMode`` (lowercase
#: wire form); a value outside the set is rejected at parse time.
SearchMode = Literal["tsquery", "trgm"]


class SearchQuery(_Camel):
    """Full-text search terminal: ``{index, query, filter?, mode?, snippet?}``.

    ``filter`` is the db-side ``FilterExpr`` (the same type ``.filter()`` and
    ``authorize`` use), narrowing search results server-side. ``mode`` selects
    the match strategy (FM-30): omitted/``"tsquery"`` is today's full-text
    behavior; ``"trgm"`` is substring matching over the index's text fields
    (see ``SearchMode``). ``snippet`` (FM-31) opts each hit into a
    ``_searchSnippet`` field — a server-rendered ``ts_headline`` fragment with
    matched terms wrapped in ``<mark>...</mark>``; tsquery mode only (the
    server rejects it with ``mode="trgm"``). ``query`` honors web search
    operators (FM-31): quoted phrases require adjacency, the bare word ``or``
    unions, and ``-term`` excludes. ``filter``/``mode``/``snippet`` are omitted
    on the wire when ``None`` (mirrors the server's
    ``#[serde(skip_serializing_if = "Option::is_none")]``), so existing
    requests stay byte-identical. ``VectorSearchQuery.filter`` is the same full
    ``FilterExpr`` type.
    """

    index: str
    query: str
    filter: FilterExpr | None = None
    mode: SearchMode | None = None
    snippet: bool | None = None

    @model_serializer(mode="wrap")
    def _drop_none_optionals(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("filter") is None:
            out.pop("filter", None)
        if out.get("mode") is None:
            out.pop("mode", None)
        if out.get("snippet") is None:
            out.pop("snippet", None)
        return out


class AggregateOp:
    """Lowercase aggregate-op wire tags for the ``aggregate`` terminal.

    Mirrors ``server/src/query.rs::AggregateOp`` (``#[serde(rename_all =
    "lowercase")]``) — the five SQL aggregates this terminal can run. ``count``
    aggregates rows and consumes no aggregate field. A plain class because
    Python's ``Literal`` already gives us the closed domain; this is just the
    canonical string constants.
    """

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"


class AggregateSpec(_Camel):
    """``aggregate`` terminal spec: ``{op, groupBy?}``.

    ``op`` selects the SQL aggregate run over the index field after the eq
    prefix; ``groupBy`` (camelCase on the wire) shifts the terminal to a grouped
    aggregate. ``count`` aggregates rows and consumes no aggregate field (a
    scalar ``count`` needs no index at all; a grouped ``count`` needs one index
    field beyond the eq prefix to group by). Mirrors
    ``server/src/query.rs::AggregateSpec`` byte-for-byte. ``groupBy`` is omitted
    on the wire when ``False`` (mirrors the TS/Rust clients' skip-when-false
    convention; the server accepts either form).
    """

    op: Literal["sum", "avg", "min", "max", "count"]
    group_by: bool = False

    @model_serializer(mode="wrap")
    def _drop_false_group_by(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("groupBy") is False:
            out.pop("groupBy", None)
        return out


class AggregateGroup(_Camel):
    """One ``{key, value}`` row from a grouped ``aggregate`` terminal.

    Mirrors ``server/src/query.rs::AggregateGroup`` byte-for-byte (camelCase).
    """

    key: Any
    value: Any


class VectorSearchQuery(_Camel):
    """Vector-similarity terminal: ``{index, vector, limit, filter?}``.

    ``filter`` is the db-side ``FilterExpr`` (the same type ``.filter()``,
    ``authorize``, and ``SearchQuery`` use), narrowing vector-search results
    server-side. It is omitted on the wire when ``None`` (mirrors the server's
    ``#[serde(skip_serializing_if = "Option::is_none")]``), so existing requests
    stay byte-identical. Mirrors ``SearchQuery.filter`` exactly.
    """

    index: str
    vector: list[float]
    limit: int
    filter: FilterExpr | None = None

    @model_serializer(mode="wrap")
    def _drop_none_filter(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("filter") is None:
            out.pop("filter", None)
        return out


class HybridSearchQuery(_Camel):
    """Hybrid search terminal: ``{query, vector, limit, searchIndex?, vectorIndex?, k?}``.

    Fuses full-text (``search``) and vector (``vectorSearch``) ranking over the
    same table via Reciprocal Rank Fusion (RRF). The table must declare BOTH a
    search index (tsvector) and a vector index. ``search_index``/``vector_index``
    optionally name the indexes (auto-selected server-side when ``None``); ``k``
    is the RRF constant (default 60). Mirrors
    ``server/src/query.rs::HybridSearchQuery`` byte-for-byte (camelCase,
    ``extra='forbid'``). The optional fields are omitted on the wire when
    ``None`` (mirrors the server's ``Option::is_none`` skip rule).
    """

    query: str
    vector: list[float]
    limit: int
    search_index: str | None = None
    vector_index: str | None = None
    k: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_optionals(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        for alias in ("searchIndex", "vectorIndex", "k"):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


# --- ClientMessage (client -> server WS vocabulary; discriminator "type") ---
#
# Mirrors ``server/src/protocol.rs::ClientMessage`` (the ``#[serde(tag = "type",
# rename_all = "camelCase", deny_unknown_fields)]`` enum). ``extra="forbid"`` is
# inherited from ``_Camel``. ``query``/``txn`` are deliberately ``dict[str, Any]``
# rather than ``Query``/``Transaction`` so the wire layer stays decoupled from the
# DSL layer (avoids a circular import with ``query.py``/``mutation.py``) and so
# future DSL extensions pass through unchanged until they're re-serialized.


class _ClientAuth(_Camel):
    type: Literal["auth"] = "auth"
    # SEC-001 phase 2: optional — a browser dashboard authenticates over `/sync`
    # from the HttpOnly cookie, sending only `db`. CLI/SDK/machine tokens still
    # send `token`; backward-compatible. Omitted on the wire when None, mirroring
    # the server/rust `skip_serializing_if = "Option::is_none"`.
    token: str | None = None
    db: str
    # ARC-013: the protocol version this client speaks. Absent ⇒ version 1. A
    # value greater than the server's ``PROTOCOL_VERSION`` is rejected with
    # ``UNSUPPORTED_PROTOCOL``. Omitted on the wire when ``None``, mirroring
    # the server/rust ``skip_serializing_if = "Option::is_none"``.
    protocol_version: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_token(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("token") is None:
            out.pop("token", None)
        if out.get("protocolVersion") is None:
            out.pop("protocolVersion", None)
        return out


class _ClientSubscribe(_Camel):
    type: Literal["subscribe"] = "subscribe"
    query_id: str
    query: dict[str, Any]


class _ClientUnsubscribe(_Camel):
    type: Literal["unsubscribe"] = "unsubscribe"
    query_id: str


class _ClientMutate(_Camel):
    type: Literal["mutate"] = "mutate"
    mut_id: str
    idempotency_key: str | None = None
    txn: dict[str, Any]

    @model_serializer(mode="wrap")
    def _drop_idempotency_when_none(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("idempotencyKey") is None:
            out.pop("idempotencyKey", None)
        return out


class _ClientSchedule(_Camel):
    type: Literal["schedule"] = "schedule"
    schedule_id: str
    when: ScheduleWhen
    txn: dict[str, Any]


class _ClientCancelSchedule(_Camel):
    type: Literal["cancelSchedule"] = "cancelSchedule"
    schedule_id: str
    id: str


class _ClientPauseSchedule(_Camel):
    type: Literal["pauseSchedule"] = "pauseSchedule"
    schedule_id: str
    id: str


class _ClientResumeSchedule(_Camel):
    type: Literal["resumeSchedule"] = "resumeSchedule"
    schedule_id: str
    id: str


class _ClientListSchedules(_Camel):
    type: Literal["listSchedules"] = "listSchedules"
    schedule_id: str


class _ClientStartWorkflow(_Camel):
    """FM-29 start a workflow run. ``spec`` snapshots the definition."""

    type: Literal["startWorkflow"] = "startWorkflow"
    workflow_id: str
    spec: WorkflowSpec


class _ClientCancelWorkflow(_Camel):
    """FM-29 cancel a pending/running workflow run by id."""

    type: Literal["cancelWorkflow"] = "cancelWorkflow"
    workflow_id: str
    id: str


class _ClientSignalWorkflow(_Camel):
    """Deliver a named signal to a waiting run (``awaitSignal`` steps). The
    reply reuses ``workflowAck``. ``payload`` (the value handed to the step
    outcome's ``signal``) is omitted on the wire when ``None``."""

    type: Literal["signalWorkflow"] = "signalWorkflow"
    workflow_id: str
    id: str
    name: str
    payload: Any | None = None

    @model_serializer(mode="wrap")
    def _drop_none_payload(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("payload") is None:
            out.pop("payload", None)
        return out


class _ClientListWorkflows(_Camel):
    """FM-29 list runs, newest first. ``status`` is omitted on the wire when
    ``None`` (no filter)."""

    type: Literal["listWorkflows"] = "listWorkflows"
    workflow_id: str
    status: WorkflowStatus | None = None

    @model_serializer(mode="wrap")
    def _drop_none_status(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("status") is None:
            out.pop("status", None)
        return out


class _ClientPing(_Camel):
    type: Literal["ping"] = "ping"


class _ClientPresence(_Camel):
    """ENH-015 join a presence room. ``state`` is omitted on the wire when
    ``None`` (mirrors the server's ``skip_serializing_if = "Option::is_none"``)."""

    type: Literal["presence"] = "presence"
    room: str
    state: Any | None = None

    @model_serializer(mode="wrap")
    def _drop_none_state(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("state") is None:
            out.pop("state", None)
        return out


class _ClientPresenceState(_Camel):
    """ENH-015 broadcast updated presence state for this connection in ``room``.

    ``ttl_ms`` (ENH-015 follow-up) arms a per-state expiry; omitted on the wire
    when ``None`` (the server clears ``state`` to ``null`` ``ttlMs`` after the
    last refresh; the member stays)."""

    type: Literal["presenceState"] = "presenceState"
    room: str
    state: Any
    ttl_ms: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_ttl(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("ttlMs") is None:
            out.pop("ttlMs", None)
        return out


class _ClientLeavePresence(_Camel):
    """ENH-015 leave a presence room."""

    type: Literal["leavePresence"] = "leavePresence"
    room: str


ClientMessage = Annotated[
    (
        _ClientAuth
        | _ClientSubscribe
        | _ClientUnsubscribe
        | _ClientMutate
        | _ClientSchedule
        | _ClientCancelSchedule
        | _ClientPauseSchedule
        | _ClientResumeSchedule
        | _ClientListSchedules
        | _ClientStartWorkflow
        | _ClientCancelWorkflow
        | _ClientSignalWorkflow
        | _ClientListWorkflows
        | _ClientPresence
        | _ClientPresenceState
        | _ClientLeavePresence
        | _ClientPing
    ),
    Field(discriminator="type"),
]


# --- PresenceMember (ENH-015; mirrors server protocol.rs::PresenceMember) ----
#
# One entry in a presence room's member list. ``connectionId`` is the opaque,
# unique-per-session key; ``user`` carries display identity; ``state`` is an
# opaque client-supplied blob (always present on the wire — ``None`` serializes
# as JSON ``null``, mirroring the server's ``serde_json::Value`` which has no
# notion of absence).


class PresenceMember(_Camel):
    """One entry in a presence room's member list.

    Attributes:
        connection_id: The opaque, unique-per-session key.
        user: Display identity for this member.
        state: Opaque client-supplied blob. Always present on the wire —
            ``None`` serializes as JSON ``null``, mirroring the server's
            ``serde_json::Value`` which has no notion of absence.
    """

    connection_id: str
    user: AuthedUser
    state: Any


# --- ServerMessage (server -> client WS vocabulary; discriminator "type") ---
#
# Mirrors ``server/src/protocol.rs::ServerMessage`` (the ``#[serde(tag = "type",
# rename_all = "camelCase", rename_all_fields = "camelCase")]`` enum). The Rust
# enum does not set ``deny_unknown_fields`` at the top level, but each leaf
# struct derives it, so inheriting ``extra="forbid"`` from ``_Camel`` keeps the
# leaf shapes tight; validation routes through ``TypeAdapter(ServerMessage)``
# because the alias itself has no ``model_validate``.
# Embedded errors are the ``{code, message}`` envelope (a small
# ``_ErrorEnvelope`` model), not ``RtDbError`` (which is an Exception).
# ``queryUpdate.result`` and ``mutateOk.results[]`` are opaque JSON (``object`` /
# ``list[object]``); the untagged ``QueryResult`` parsing happens in Task 9.


class _ErrorEnvelope(_Camel):
    """The ``{code, message}`` body embedded in WS error frames."""

    code: str
    message: str


class BatchQueryOutcome(_Camel):
    """One slot of a ``POST /api/query-batch`` response.

    Exactly one of ``result`` / ``error`` accompanies ``ok``: an ok slot is
    ``{ok: true, result: ...}``; an errored slot is ``{ok: false, error: {...}}``
    (``result``/``error`` are omitted on the wire when ``None``). ``result`` is
    the untagged ``QueryResult`` — decode it per-query with
    ``query.parse_result(model, terminal, outcome.result)``. Mirrors
    ``server/src/http_api.rs::BatchQueryOutcome`` and ``rust-client``'s wire type.
    """

    ok: bool
    result: object | None = None
    error: _ErrorEnvelope | None = None


class _ServerAuthOk(_Camel):
    type: Literal["authOk"] = "authOk"
    user: AuthedUser
    # ARC-013: the server's ``PROTOCOL_VERSION``, present only when this
    # client's ``auth`` frame carried ``protocolVersion``. Omitted on the wire
    # when ``None`` (mirrors server/rust ``skip_serializing_if``).
    protocol_version: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_protocol_version(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("protocolVersion") is None:
            out.pop("protocolVersion", None)
        return out


class _ServerAuthErr(_Camel):
    type: Literal["authErr"] = "authErr"
    error: _ErrorEnvelope


class _ServerQueryUpdate(_Camel):
    type: Literal["queryUpdate"] = "queryUpdate"
    query_id: str
    result: object  # untagged QueryResult; parsed by query.py (Task 9)


class _ServerMutateOk(_Camel):
    type: Literal["mutateOk"] = "mutateOk"
    mut_id: str
    results: list[object]


class _ServerMutateErr(_Camel):
    type: Literal["mutateErr"] = "mutateErr"
    mut_id: str
    error: _ErrorEnvelope


class _ServerSubscribeErr(_Camel):
    type: Literal["subscribeErr"] = "subscribeErr"
    query_id: str
    error: _ErrorEnvelope


class _ServerScheduleOk(_Camel):
    type: Literal["scheduleOk"] = "scheduleOk"
    schedule_id: str
    id: str


class _ServerScheduleErr(_Camel):
    type: Literal["scheduleErr"] = "scheduleErr"
    schedule_id: str
    error: _ErrorEnvelope


class _ServerScheduleAck(_Camel):
    type: Literal["scheduleAck"] = "scheduleAck"
    schedule_id: str
    ok: bool
    error: _ErrorEnvelope | None = None

    @model_serializer(mode="wrap")
    def _drop_error_when_none(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("error") is None:
            out.pop("error", None)
        return out


class _ServerListSchedulesOk(_Camel):
    type: Literal["listSchedulesOk"] = "listSchedulesOk"
    schedule_id: str
    schedules: list[ScheduleInfo]


class _ServerStartWorkflowOk(_Camel):
    """FM-29 run accepted — carries the re-read row's info projection."""

    type: Literal["startWorkflowOk"] = "startWorkflowOk"
    workflow_id: str
    info: WorkflowInfo


class _ServerStartWorkflowErr(_Camel):
    """FM-29 the start op (and listWorkflows failures) were rejected. The
    server types list failures as this same frame — there is no distinct
    list-error frame."""

    type: Literal["startWorkflowErr"] = "startWorkflowErr"
    workflow_id: str
    error: _ErrorEnvelope


class _ServerWorkflowAck(_Camel):
    """FM-29 reply to cancelWorkflow AND signalWorkflow (a failed delivery —
    unknown run, not waiting, name mismatch — arrives as ``ok: false`` plus the
    error envelope). ``error`` is omitted on the wire when ``ok``."""

    type: Literal["workflowAck"] = "workflowAck"
    workflow_id: str
    ok: bool
    error: _ErrorEnvelope | None = None

    @model_serializer(mode="wrap")
    def _drop_error_when_none(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("error") is None:
            out.pop("error", None)
        return out


class _ServerListWorkflowsOk(_Camel):
    type: Literal["listWorkflowsOk"] = "listWorkflowsOk"
    workflow_id: str
    workflows: list[WorkflowInfo]


class _ServerPresenceSnapshot(_Camel):
    """ENH-015 fan-out of a room's current member list (server→client)."""

    type: Literal["presenceSnapshot"] = "presenceSnapshot"
    room: str
    members: list[PresenceMember]


class _ServerPresenceErr(_Camel):
    """ENH-015 the server rejected a presence op for ``room``."""

    type: Literal["presenceErr"] = "presenceErr"
    room: str
    error: _ErrorEnvelope


class _ServerPong(_Camel):
    type: Literal["pong"] = "pong"


ServerMessage = Annotated[
    (
        _ServerAuthOk
        | _ServerAuthErr
        | _ServerQueryUpdate
        | _ServerMutateOk
        | _ServerMutateErr
        | _ServerSubscribeErr
        | _ServerScheduleOk
        | _ServerScheduleErr
        | _ServerScheduleAck
        | _ServerListSchedulesOk
        | _ServerStartWorkflowOk
        | _ServerStartWorkflowErr
        | _ServerWorkflowAck
        | _ServerListWorkflowsOk
        | _ServerPresenceSnapshot
        | _ServerPresenceErr
        | _ServerPong
    ),
    Field(discriminator="type"),
]
