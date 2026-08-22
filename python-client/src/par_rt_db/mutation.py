"""Transaction DSL: ``Step`` (14 ops), ``StepResult``, ``Transaction``,
and the ``Mutation`` builder.

Mirrors ``server/src/txn.rs`` (the ``Step`` enum + ``Transaction`` struct +
untagged ``StepResult``) and the builder ergonomics of
``ts-client/src/mutation.ts`` / ``rust-client/src/mutation.rs``.

Wire shapes (load-bearing — match the server exactly):

* ``Step`` is tagged by ``op`` (camelCase variants: ``insert``/``patch``/
  ``replace``/``delete``/``undelete``/``expectVersion``/``expectAbsent``/
  ``upsert``/``patchByQuery``/``deleteByQuery``/``schedule``/
  ``cancelSchedule``/``startWorkflow``/``cancelWorkflow``) with
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
from .wire import AwaitSignalSpec, FilterExpr, ScheduleWhen, WorkflowSpec, to_camel

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


class _Undelete(_Step):
    """FM-33: restore a soft-deleted row (only legal on a ``softDelete``
    table). ``NOT_FOUND`` when the row is absent; idempotent ``None`` result
    when it is present and already live. Step result is ``None``."""

    op: Literal["undelete"] = "undelete"
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


class _StartWorkflow(_Step):
    """FM-29: start a workflow run from ``spec``. The run is inserted on the
    open transaction — a rolled-back txn leaves no orphan run. Step result
    ``{"workflowId": "<id>"}``."""

    op: Literal["startWorkflow"] = "startWorkflow"
    spec: WorkflowSpec


class _CancelWorkflow(_Step):
    """FM-29: cancel a workflow run by id. Step result ``{"cancelled": bool}`` —
    ``False`` when missing or already terminal (not an error)."""

    op: Literal["cancelWorkflow"] = "cancelWorkflow"
    id: str


#: Discriminated union of all 14 step ops. The ``op`` literal drives dispatch;
#: ``deny_unknown_fields`` is per-variant via ``extra="forbid"`` on ``_Step``.
Step = Annotated[
    (
        _Insert
        | _Patch
        | _Replace
        | _Delete
        | _Undelete
        | _ExpectVersion
        | _ExpectAbsent
        | _Upsert
        | _PatchByQuery
        | _DeleteByQuery
        | _Schedule
        | _CancelSchedule
        | _StartWorkflow
        | _CancelWorkflow
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


class _StepStartWorkflowResult(BaseModel):
    """Per-step result for ``startWorkflow``: ``{"workflowId"}`` — the id of
    the inserted run."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )

    workflow_id: str


#: Untagged per-step result, positionally aligned with ``Transaction.steps``.
#: Variant order matters: ``_StepUpsert`` (richer) must precede ``_StepInsert``
#: so ``{"id","inserted"}`` is captured as an upsert, not silently trimmed to an
#: insert. ``None`` covers the ``null`` wire shape produced by ``expectVersion``/
#: ``expectAbsent``/``patch``/``replace``/``delete``. ``patchByQuery``/
#: ``deleteByQuery`` carry their own ``{patched|deleted, truncated}`` shape;
#: ``schedule``/``cancelSchedule`` carry ``{scheduleId}`` / ``{cancelled}``;
#: ``startWorkflow``/``cancelWorkflow`` carry ``{workflowId}`` /
#: ``{cancelled}`` (the latter shares the cancelSchedule shape).
StepResult = (
    _StepUpsert
    | _StepInsert
    | _StepPatchByQuery
    | _StepDeleteByQuery
    | _StepScheduleResult
    | _StepCancelScheduleResult
    | _StepStartWorkflowResult
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
        """Delete step: remove the document at ``id`` in ``table`` (a soft-delete
        table gets a ``deleted_at`` stamp instead — FM-33)."""
        self._steps.append(_Delete(table=table, id=id))
        return self

    def undelete(self, table: str, id: str) -> _MutationBuilder:
        """Undelete step (FM-33): restore the soft-deleted document at ``id`` in
        ``table``. ``NOT_FOUND`` when the row is absent; idempotent when already
        live. Rejected (``BAD_REQUEST``) on a table that does not declare
        ``softDelete``."""
        self._steps.append(_Undelete(table=table, id=id))
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
        """Schedule step: enqueue ``txn`` to run at ``when`` (one-shot, cron, or
        interval). The nested steps do not run in this transaction — the server
        fires them at the due time; the step result carries the job's
        ``scheduleId``."""
        self._steps.append(_Schedule(op="schedule", when=when, txn=txn))
        return self

    def cancel_schedule(self, id: str) -> _MutationBuilder:
        """Cancel-schedule step: remove the pending scheduled job ``id``. The
        step result reports ``{"cancelled": bool}`` — ``False`` (not an error)
        when no such job is pending."""
        self._steps.append(_CancelSchedule(op="cancelSchedule", id=id))
        return self

    def start_workflow(self, spec: WorkflowSpec) -> _MutationBuilder:
        """Start-workflow step (FM-29): insert a workflow run from ``spec`` on
        the open transaction. The step result carries the run's
        ``{"workflowId": "<id>"}``."""
        self._steps.append(_StartWorkflow(op="startWorkflow", spec=spec))
        return self

    def cancel_workflow(self, id: str) -> _MutationBuilder:
        """Cancel-workflow step (FM-29): cancel the run ``id``. The step result
        reports ``{"cancelled": bool}`` — ``False`` (not an error) when the run
        is missing or already terminal."""
        self._steps.append(_CancelWorkflow(op="cancelWorkflow", id=id))
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


def await_signal(name: str, timeout_ms: int | None = None) -> AwaitSignalSpec:
    """Build an ``awaitSignal`` wait declaration for a
    :class:`~par_rt_db.wire.WorkflowStepSpec` — the spec-level counterpart of
    the builder's step constructors::

        WorkflowSpec(
            name="gate",
            steps=[
                WorkflowStepSpec(txn=txn.model_dump(by_alias=True)),
                WorkflowStepSpec(await_signal=await_signal("approve", timeout_ms=60_000)),
            ],
        )

    The step parks the run until a signal named ``name`` is delivered
    (``signal_workflow`` on the clients); ``timeout_ms`` bounds each wait
    attempt — omitted means wait indefinitely (cancel is the only escape).
    A timed-out attempt retries the FULL timeout again (never backoff)."""
    return AwaitSignalSpec(name=name, timeout_ms=timeout_ms)


# Resolve deferred annotations (``from __future__ import annotations`` makes
# every annotation a string; ``model_rebuild`` evaluates them so the
# discriminated-union ``Step`` schema is fully built before first use).
Transaction.model_rebuild()
