"""Transaction DSL: ``Step`` (11 ops), ``StepResult``, ``Transaction``,
and the ``Mutation`` builder.

Mirrors ``server/src/txn.rs`` (the ``Step`` enum + ``Transaction`` struct +
untagged ``StepResult``) and the builder ergonomics of
``ts-client/src/mutation.ts`` / ``rust-client/src/mutation.rs``.

Wire shapes (load-bearing — match the server exactly):

* ``Step`` is tagged by ``op`` (camelCase variants: ``insert``/``patch``/
  ``replace``/``delete``/``expectVersion``/``expectAbsent``/``upsert``/
  ``patchByQuery``/``deleteByQuery``/``schedule``/``cancelSchedule``) with
  ``deny_unknown_fields`` mirrored via ``extra="forbid"``.
* ``Transaction`` is ``{"steps": Step[]}``; the server caps at 1024 steps —
  enforced client-side by the builder so an over-cap transaction never reaches
  the wire.
* ``StepResult`` is untagged: ``{"id", "inserted"}`` (upsert) beats ``{"id"}``
  (insert) beats ``None`` — Union variant ORDER matters (richest first),
  mirroring the rust-client's ``#[serde(untagged)]`` declaration order. (Pydantic
  v2's smart-union mode would already pick the closer match, but locking in the
  order makes the contract explicit and survives a future ``left_to_right`` mode.)

``StepResult`` is a plain ``Union`` type alias (no ``Annotated``) because the
server's ``#[serde(untagged)]`` carries no discriminator; validation routes
through ``TypeAdapter(StepResult)`` (the alias has no ``model_validate``) —
same pattern as ``FilterExpr`` / ``ClientMessage`` in ``wire.py``.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from .errors import ErrorCode, RtDbError
from .wire import FilterExpr, ScheduleWhen, to_camel

#: Client-side cap on transaction length. Mirrors ``server/src/txn.rs::MAX_STEPS``
#: (1024); the server rejects anything longer, so the builder raises eagerly to
#: keep the over-cap payload off the wire.
MAX_STEPS = 1024


class _Step(BaseModel):
    """Base for ``Step`` variants: camelCase wire keys, reject unknown fields."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )


class _Insert(_Step):
    op: Literal["insert"] = "insert"
    table: str
    doc: dict[str, Any]


class _Patch(_Step):
    op: Literal["patch"] = "patch"
    table: str
    id: str
    fields: dict[str, Any]


class _Replace(_Step):
    op: Literal["replace"] = "replace"
    table: str
    id: str
    doc: dict[str, Any]


class _Delete(_Step):
    op: Literal["delete"] = "delete"
    table: str
    id: str


class _ExpectVersion(_Step):
    op: Literal["expectVersion"] = "expectVersion"
    table: str
    id: str
    version: int


class _ExpectAbsent(_Step):
    op: Literal["expectAbsent"] = "expectAbsent"
    table: str
    index: str
    eq: list[Any]


class _Upsert(_Step):
    op: Literal["upsert"] = "upsert"
    table: str
    index: str
    eq: list[Any]
    insert: dict[str, Any]
    patch: dict[str, Any]


class _PatchByQuery(_Step):
    op: Literal["patchByQuery"] = "patchByQuery"
    table: str
    filter: FilterExpr
    patch: dict[str, Any]
    #: Optional row cap (default ``MAX_BY_QUERY_ROWS`` server-side). Omitted on
    #: the wire when ``None`` (mirrors the server's
    #: ``skip_serializing_if = "Option::is_none"``).
    limit: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_limit(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("limit") is None:
            out.pop("limit", None)
        return out


class _DeleteByQuery(_Step):
    op: Literal["deleteByQuery"] = "deleteByQuery"
    table: str
    filter: FilterExpr
    #: Optional row cap (default ``MAX_BY_QUERY_ROWS`` server-side). Omitted on
    #: the wire when ``None`` (mirrors the server's
    #: ``skip_serializing_if = "Option::is_none"``).
    limit: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_limit(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("limit") is None:
            out.pop("limit", None)
        return out


class _Schedule(_Step):
    """FM-28: schedule a nested txn. The nested steps do NOT run at enqueue —
    the server's scheduler fires them at ``when``; the in-memory harness's
    ``tick()`` mirrors that."""

    op: Literal["schedule"] = "schedule"
    when: ScheduleWhen
    txn: Transaction


class _CancelSchedule(_Step):
    """FM-28: cancel a previously-enqueued scheduled job by id."""

    op: Literal["cancelSchedule"] = "cancelSchedule"
    id: str


#: Discriminated union of all 11 step ops. The ``op`` literal drives dispatch;
#: ``deny_unknown_fields`` is per-variant via ``extra="forbid"`` on ``_Step``.
Step = Annotated[
    (
        _Insert
        | _Patch
        | _Replace
        | _Delete
        | _ExpectVersion
        | _ExpectAbsent
        | _Upsert
        | _PatchByQuery
        | _DeleteByQuery
        | _Schedule
        | _CancelSchedule
    ),
    Field(discriminator="op"),
]


class Transaction(BaseModel):
    """A transaction: an ordered list of steps.

    The server caps the step count at 1024 (``server/src/txn.rs::MAX_STEPS``);
    the ``Mutation`` builder enforces the same cap client-side so an over-cap
    transaction never reaches the wire. Direct ``model_validate`` callers (e.g.
    parsing server-emitted payloads) bypass the builder — keep those honest by
    construction.
    """

    model_config = ConfigDict(extra="forbid")

    steps: list[Step]


class _StepInsert(BaseModel):
    """Per-step result for ``insert``: ``{"id"}`` only."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )

    id: str


class _StepUpsert(BaseModel):
    """Per-step result for ``upsert``: ``{"id", "inserted"}``.

    Declared BEFORE ``_StepInsert`` so a payload like ``{"id", "inserted"}``
    matches this richer variant first — mirrors the rust-client's
    ``#[serde(untagged)]`` order where ``Upsert`` precedes ``Insert`` so serde's
    left-to-right probe doesn't greedily capture-and-trim to ``Insert``.
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )

    id: str
    inserted: bool


class _StepPatchByQuery(BaseModel):
    """Per-step result for ``patchByQuery``: ``{"patched", "truncated"}``."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )

    patched: int
    truncated: bool


class _StepDeleteByQuery(BaseModel):
    """Per-step result for ``deleteByQuery``: ``{"deleted", "truncated"}``."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )

    deleted: int
    truncated: bool


class _StepScheduleResult(BaseModel):
    """Per-step result for ``schedule``: ``{"scheduleId"}`` — the id of the
    enqueued job (cancellable via a later ``cancelSchedule`` step)."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )

    schedule_id: str


class _StepCancelScheduleResult(BaseModel):
    """Per-step result for ``cancelSchedule``: ``{"cancelled"}`` — ``False``
    when no job with that id was pending (not an error, unlike the standalone
    cancel op's ``NOT_FOUND``)."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )

    cancelled: bool


#: Untagged per-step result, positionally aligned with ``Transaction.steps``.
#: Variant order matters: ``_StepUpsert`` (richer) must precede ``_StepInsert``
#: so ``{"id","inserted"}`` is captured as an upsert, not silently trimmed to an
#: insert. ``None`` covers the ``null`` wire shape produced by ``expectVersion``/
#: ``expectAbsent``/``patch``/``replace``/``delete``. ``patchByQuery``/
#: ``deleteByQuery`` carry their own ``{patched|deleted, truncated}`` shape;
#: ``schedule``/``cancelSchedule`` carry ``{scheduleId}`` / ``{cancelled}``.
StepResult = (
    _StepUpsert
    | _StepInsert
    | _StepPatchByQuery
    | _StepDeleteByQuery
    | _StepScheduleResult
    | _StepCancelScheduleResult
    | None
)


class _MutationBuilder:
    """Fluent builder producing a wire-shaped ``Transaction``.

    Each method appends one step and returns ``self`` for chaining. ``build``
    enforces the client-side ``MAX_STEPS`` cap (matching the server) before
    materializing the ``Transaction`` so an over-cap payload never reaches the
    wire.
    """

    def __init__(self) -> None:
        self._steps: list[Step] = []

    def insert(self, table: str, doc: dict[str, Any]) -> _MutationBuilder:
        """Insert step: create a new document in ``table``. The server assigns
        ``id``/``created_at``."""
        self._steps.append(_Insert(table=table, doc=doc))
        return self

    def patch(self, table: str, id: str, fields: dict[str, Any]) -> _MutationBuilder:
        """Patch step: merge ``fields`` into the existing document at ``id`` in ``table``."""
        self._steps.append(_Patch(table=table, id=id, fields=fields))
        return self

    def replace(self, table: str, id: str, doc: dict[str, Any]) -> _MutationBuilder:
        """Replace step: overwrite the document at ``id`` in ``table`` with ``doc``."""
        self._steps.append(_Replace(table=table, id=id, doc=doc))
        return self

    def delete(self, table: str, id: str) -> _MutationBuilder:
        """Delete step: remove the document at ``id`` in ``table``."""
        self._steps.append(_Delete(table=table, id=id))
        return self

    def expect_version(self, table: str, id: str, version: int) -> _MutationBuilder:
        """Expect-version precondition: the transaction aborts unless the
        document at ``id`` is currently at ``version``."""
        self._steps.append(_ExpectVersion(table=table, id=id, version=version))
        return self

    def expect_absent(self, table: str, index: str, eq: list[Any]) -> _MutationBuilder:
        """Expect-absent precondition: the transaction aborts unless no document
        matches the ``index``/``eq`` equality prefix."""
        self._steps.append(_ExpectAbsent(table=table, index=index, eq=eq))
        return self

    def upsert(
        self,
        table: str,
        index: str,
        eq: list[Any],
        insert: dict[str, Any],
        patch: dict[str, Any],
    ) -> _MutationBuilder:
        """Upsert step: if a document matches the ``index``/``eq`` prefix, apply
        ``patch``; otherwise insert ``insert``."""
        self._steps.append(
            _Upsert(
                table=table,
                index=index,
                eq=eq,
                insert=insert,
                patch=patch,
            )
        )
        return self

    def patch_by_query(
        self,
        table: str,
        filter: FilterExpr,
        patch: dict[str, Any],
        limit: int | None = None,
    ) -> _MutationBuilder:
        """Patch-by-query step: merge ``patch`` into every document in ``table``
        matching ``filter``. ``limit`` caps the affected rows (default
        ``MAX_BY_QUERY_ROWS`` server-side); a truncated run reports it."""
        self._steps.append(_PatchByQuery(table=table, filter=filter, patch=patch, limit=limit))
        return self

    def delete_by_query(
        self,
        table: str,
        filter: FilterExpr,
        limit: int | None = None,
    ) -> _MutationBuilder:
        """Delete-by-query step: remove every document in ``table`` matching
        ``filter``. ``limit`` caps the affected rows (default
        ``MAX_BY_QUERY_ROWS`` server-side); a truncated run reports it."""
        self._steps.append(_DeleteByQuery(table=table, filter=filter, limit=limit))
        return self

    def schedule(self, when: ScheduleWhen, txn: Transaction) -> _MutationBuilder:
        """Schedule step: enqueue ``txn`` to run at ``when`` (one-shot or cron).
        The nested steps do not run in this transaction — the server fires them
        at the due time; the step result carries the job's ``scheduleId``."""
        self._steps.append(_Schedule(op="schedule", when=when, txn=txn))
        return self

    def cancel_schedule(self, id: str) -> _MutationBuilder:
        """Cancel-schedule step: remove the pending scheduled job ``id``. The
        step result reports ``{"cancelled": bool}`` — ``False`` (not an error)
        when no such job is pending."""
        self._steps.append(_CancelSchedule(op="cancelSchedule", id=id))
        return self

    def build(self) -> Transaction:
        """Materialize the ``Transaction``, enforcing the client-side
        ``MAX_STEPS`` cap so an over-cap payload never reaches the wire."""
        if len(self._steps) > MAX_STEPS:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"transaction exceeds max {MAX_STEPS} steps",
            )
        return Transaction(steps=self._steps)


class _MutationNamespace:
    """Namespace for ``Mutation`` so callers write ``Mutation.builder()`` /
    ``Mutation.model_validate(...)`` — mirrors the rust-client's ``Mutation``
    type surface while keeping the builder and parser discoverable as a unit.

    ``builder`` and ``model_validate`` are ``staticmethod`` wrappers so the
    namespace behaves like a class with classmethods from the caller's side.
    """

    builder = staticmethod(_MutationBuilder)
    model_validate = staticmethod(Transaction.model_validate)


Mutation = _MutationNamespace


# Resolve deferred annotations (``from __future__ import annotations`` makes
# every annotation a string; ``model_rebuild`` evaluates them so the
# discriminated-union ``Step`` schema is fully built before first use).
Transaction.model_rebuild()
