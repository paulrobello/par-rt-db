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
    """

    kind: str
    email: str | None = None
    name: str | None = None
    github_login: str | None = None
    github_id: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_github(self, handler):  # type: ignore[no-untyped-def]
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

    ``cron``/``lastError`` are omitted on the wire when ``None``.
    """

    id: str
    kind: str
    due_at: int
    cron: str | None = None
    status: str
    last_error: str | None = None
    created_at: int
    fired_count: int

    @model_serializer(mode="wrap")
    def _drop_none_optional(self, handler):  # type: ignore[no-untyped-def]
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
    def _drop_empty_filter(self, handler):  # type: ignore[no-untyped-def]
        out = handler(self)
        if not out.get("filter"):
            out.pop("filter", None)
        return out
