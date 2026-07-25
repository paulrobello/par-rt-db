"""Wire types — the fourth implementation of par-rt-db's JSON contract.

Mirrors ``server/src/protocol.rs`` and ``server/src/query.rs`` (cross-checked
against ``ts-client/src/protocol.ts`` and ``rust-client/src/wire.rs``) field-for-field.
Discriminator unions:

* ``ScheduleWhen`` is tagged by ``type`` (camelCase variants: ``afterMs``/``runAt``/``cron``).
* ``FilterExpr`` is tagged by ``op`` (lowercase variants: ``eq``/``neq``/``gt``/``gte``/
  ``lt``/``lte``/``in``/``and``/``or``).

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


class _AfterMs(_Camel):
    type: Literal["afterMs"] = "afterMs"
    ms: int


class _RunAt(_Camel):
    type: Literal["runAt"] = "runAt"
    ms: int


class _Cron(_Camel):
    type: Literal["cron"] = "cron"
    expr: str


ScheduleWhen = Annotated[_AfterMs | _RunAt | _Cron, Field(discriminator="type")]


class ScheduleInfo(_Camel):
    """A scheduled job's public view (returned by ``listSchedules``).

    ``cron``/``lastError`` are omitted on the wire when ``None``. ``kind`` and
    ``status`` are narrowed to ``Literal`` unions (ARC-004/QA-008) mirroring the
    server's ``ScheduleKind`` / ``ScheduleStatus`` enums.
    """

    id: str
    kind: Literal["oneshot", "cron"]
    due_at: int
    cron: str | None = None
    status: Literal["pending", "running", "paused", "error"]
    last_error: str | None = None
    created_at: int
    fired_count: int

    @model_serializer(mode="wrap")
    def _drop_none_optional(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        for alias in ("cron", "lastError"):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


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
    ),
    Field(discriminator="op"),
]


class SearchQuery(_Camel):
    """Full-text search terminal: ``{index, query}``."""

    index: str
    query: str


class VectorSearchQuery(_Camel):
    """Vector-similarity terminal: ``{index, vector, limit, filter?}``.

    ``filter`` is an eq-map over the index's declared ``filterFields`` (NOT a
    ``FilterExpr``); it is omitted on the wire when ``None`` or empty, mirroring
    the server's ``BTreeMap::is_empty`` skip rule.
    """

    index: str
    vector: list[float]
    limit: int
    filter: dict[str, Any] | None = None

    @model_serializer(mode="wrap")
    def _drop_empty_filter(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if not out.get("filter"):
            out.pop("filter", None)
        return out


# --- ClientMessage (client -> server WS vocabulary; discriminator "type") ---
#
# Mirrors ``server/src/protocol.rs::ClientMessage`` (the ``#[serde(tag = "type",
# rename_all = "camelCase", deny_unknown_fields)]`` enum). ``extra="forbid"`` is
# inherited from ``_Camel``. ``query``/``txn`` reference the Query / Transaction
# models (Tasks 9-10); they are typed as ``dict[str, Any]`` for now so wire
# payloads pass through unchanged.
# TODO(tasks 9-10): tighten to Query / Transaction models.


class _ClientAuth(_Camel):
    type: Literal["auth"] = "auth"
    token: str
    db: str


class _ClientSubscribe(_Camel):
    type: Literal["subscribe"] = "subscribe"
    query_id: str
    query: dict[str, Any]  # TODO(tasks 9-10): tighten to Query


class _ClientUnsubscribe(_Camel):
    type: Literal["unsubscribe"] = "unsubscribe"
    query_id: str


class _ClientMutate(_Camel):
    type: Literal["mutate"] = "mutate"
    mut_id: str
    idempotency_key: str | None = None
    txn: dict[str, Any]  # TODO(tasks 9-10): tighten to Transaction

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
    txn: dict[str, Any]  # TODO(tasks 9-10): tighten to Transaction


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


class _ClientPing(_Camel):
    type: Literal["ping"] = "ping"


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
        | _ClientPing
    ),
    Field(discriminator="type"),
]


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


class _ServerAuthOk(_Camel):
    type: Literal["authOk"] = "authOk"
    user: AuthedUser


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
        | _ServerPong
    ),
    Field(discriminator="type"),
]
