"""Transaction DSL: ``Step`` (7 ops), ``StepResult``, ``Transaction``,
and the ``Mutation`` builder.

Mirrors ``server/src/txn.rs`` (the ``Step`` enum + ``Transaction`` struct +
untagged ``StepResult``) and the builder ergonomics of
``ts-client/src/mutation.ts`` / ``rust-client/src/mutation.rs``.

Wire shapes (load-bearing — match the server exactly):

* ``Step`` is tagged by ``op`` (camelCase variants: ``insert``/``patch``/
  ``replace``/``delete``/``expectVersion``/``expectAbsent``/``upsert``) with
  ``deny_unknown_fields`` mirrored via ``extra="forbid"``.
* ``Transaction`` is ``{"steps": Step[]}``; the server caps at 256 steps —
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

from pydantic import BaseModel, ConfigDict, Field

from .wire import to_camel

#: Client-side cap on transaction length. Mirrors ``server/src/txn.rs::MAX_STEPS``;
#: the server rejects anything longer, so the builder raises eagerly to keep
#: the over-cap payload off the wire.
MAX_STEPS = 256


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


#: Discriminated union of all 7 step ops. The ``op`` literal drives dispatch;
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
    ),
    Field(discriminator="op"),
]


class Transaction(BaseModel):
    """A transaction: an ordered list of steps.

    The server caps the step count at 256 (``server/src/txn.rs::MAX_STEPS``);
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


#: Untagged per-step result, positionally aligned with ``Transaction.steps``.
#: Variant order matters: ``_StepUpsert`` (richer) must precede ``_StepInsert``
#: so ``{"id","inserted"}`` is captured as an upsert, not silently trimmed to an
#: insert. ``None`` covers the ``null`` wire shape produced by ``expectVersion``/
#: ``expectAbsent``/``patch``/``replace``/``delete``.
StepResult = _StepUpsert | _StepInsert | None


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
        self._steps.append(_Insert(table=table, doc=doc))
        return self

    def patch(
        self, table: str, id: str, fields: dict[str, Any]
    ) -> _MutationBuilder:
        self._steps.append(_Patch(table=table, id=id, fields=fields))
        return self

    def replace(
        self, table: str, id: str, doc: dict[str, Any]
    ) -> _MutationBuilder:
        self._steps.append(_Replace(table=table, id=id, doc=doc))
        return self

    def delete(self, table: str, id: str) -> _MutationBuilder:
        self._steps.append(_Delete(table=table, id=id))
        return self

    def expect_version(
        self, table: str, id: str, version: int
    ) -> _MutationBuilder:
        self._steps.append(_ExpectVersion(table=table, id=id, version=version))
        return self

    def expect_absent(
        self, table: str, index: str, eq: list[Any]
    ) -> _MutationBuilder:
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

    def build(self) -> Transaction:
        if len(self._steps) > MAX_STEPS:
            raise ValueError(
                f"transaction exceeds max {MAX_STEPS} steps"
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
