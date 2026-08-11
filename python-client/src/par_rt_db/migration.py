"""Declarative schema migration: ``Cast``, ``Directive`` (8 ops), ``MigrateRequest``,
and the ``Migration`` builder.

Mirrors ``server/src/migrate.rs`` (the ``Directive`` enum + ``Cast`` +
``MigrateRequest``) and the builder ergonomics of
``ts-client/src/migration.ts`` / ``rust-client/src/migration.rs``.

Wire shapes (load-bearing — match the server exactly):

* ``Directive`` is tagged by ``op`` (camelCase variants: ``renameField`` /
  ``renameTable`` / ``changeType`` / ``dropField`` / ``dropTable`` /
  ``dropIndex`` / ``setDefault`` / ``evalExpr``) with ``deny_unknown_fields``
  mirrored via ``extra="forbid"`` — the same shape contract as ``mutation.Step``.
* ``Cast`` is the closed set of sound coercions for ``changeType``
  (``toString`` / ``toNumber`` / ``toInt64`` / ``toBoolean``).
* ``MigrateRequest`` is ``{"directives": Directive[], "dryRun": bool}``;
  ``dryRun`` defaults to ``False`` (server's ``#[serde(default)]``).
* ``evalExpr.where`` is the wire alias for the server's ``where_clause`` field
  (serde ``rename = "where"``); ``changeType.default`` and ``evalExpr.where``
  are omitted on the wire when unset, matching the ts-client's omit convention.

``from`` and ``where`` are Python keywords, so the corresponding fields are
``from_`` / ``where_`` with explicit wire aliases (``Field(alias="from")`` /
``Field(alias="where")``). The ``_Camel`` ``alias_generator`` is left in place
for the remaining camelCase fields.

Response models (``MigrateResult`` / ``DirectiveReport`` / ``CastFailure`` /
``SampleChange``) live in :mod:`par_rt_db.http_client` beside the other HTTP
response types; the request/builder surface lives here.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import Field, model_serializer
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from .schema import FieldType
from .wire import _Camel


class Cast(StrEnum):
    """Closed set of sound coercions for ``Directive.changeType``.

    Mirrors ``server/src/migrate::Cast`` (camelCase on the wire). ``StrEnum``
    so pydantic v2 serializes the string value directly and
    ``Cast.TO_STRING == "toString"`` for ergonomic builder calls.
    """

    TO_STRING = "toString"
    TO_NUMBER = "toNumber"
    TO_INT64 = "toInt64"
    TO_BOOLEAN = "toBoolean"


# --- Directive (discriminator "op", camelCase) ---


class _RenameField(_Camel):
    op: Literal["renameField"] = "renameField"
    table: str
    # `from` is a Python keyword — field is `from_`, wire alias is explicit so
    # the wire key is `from` (not mangled by the alias_generator).
    from_: str = Field(alias="from")
    to: str


class _RenameTable(_Camel):
    op: Literal["renameTable"] = "renameTable"
    from_: str = Field(alias="from")
    to: str


class _ChangeType(_Camel):
    op: Literal["changeType"] = "changeType"
    table: str
    field: str
    to: FieldType
    cast: Cast
    default: Any | None = None

    @model_serializer(mode="wrap")
    def _drop_none_default(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("default") is None:
            out.pop("default", None)
        return out


class _DropField(_Camel):
    op: Literal["dropField"] = "dropField"
    table: str
    field: str


class _DropTable(_Camel):
    op: Literal["dropTable"] = "dropTable"
    name: str


class _DropIndex(_Camel):
    op: Literal["dropIndex"] = "dropIndex"
    table: str
    name: str


class _SetDefault(_Camel):
    op: Literal["setDefault"] = "setDefault"
    table: str
    field: str
    value: Any


class _EvalExpr(_Camel):
    op: Literal["evalExpr"] = "evalExpr"
    table: str
    set: str
    expr: str
    # `where` is a Python keyword — field is `where_`, wire alias is explicit.
    where_: str | None = Field(default=None, alias="where")

    @model_serializer(mode="wrap")
    def _drop_none_where(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("where") is None:
            out.pop("where", None)
        return out


#: Discriminated union of all 8 migration ops. The ``op`` literal drives
#: dispatch; ``deny_unknown_fields`` is per-variant via ``extra="forbid"`` on
#: ``_Camel`` — the same shape contract as ``mutation.Step``.
Directive = Annotated[
    (
        _RenameField
        | _RenameTable
        | _ChangeType
        | _DropField
        | _DropTable
        | _DropIndex
        | _SetDefault
        | _EvalExpr
    ),
    Field(discriminator="op"),
]


class MigrateRequest(_Camel):
    """``POST /admin/db/{db}/migrate`` request body.

    ``directives`` is the ordered list; ``dry_run`` defaults to ``False``
    (server's ``#[serde(default)]``).
    """

    directives: list[Directive]
    dry_run: bool = False


# --- Builder ---


class _MigrationBuilder:
    """Fluent builder producing a wire-shaped :class:`MigrateRequest`.

    Each method appends one directive and returns ``self`` for chaining.
    :meth:`dry_run` stashes the flag; :meth:`build` emits the full request.
    Mirrors ``rust-client::Migration`` and ``ts-client::Migration``.
    """

    def __init__(self) -> None:
        self._directives: list[Directive] = []
        self._dry_run: bool = False

    def rename_field(self, table: str, from_: str, to: str) -> _MigrationBuilder:
        """Append a ``renameField`` directive: rename field ``from_`` to ``to`` on ``table``."""
        # ``model_validate`` (not direct construction) because ``from`` is a Python
        # keyword: pydantic's pyright plugin surfaces the wire alias ``from`` as the
        # constructor keyword, which can't be expressed in Python syntax. The dict
        # form sidesteps this cleanly.
        self._directives.append(
            _RenameField.model_validate({"table": table, "from": from_, "to": to})
        )
        return self

    def rename_table(self, from_: str, to: str) -> _MigrationBuilder:
        """Append a ``renameTable`` directive: rename table ``from_`` to ``to``."""
        self._directives.append(_RenameTable.model_validate({"from": from_, "to": to}))
        return self

    def change_type(
        self,
        table: str,
        field: str,
        to: Any,
        cast: Cast,
        default: Any | None = None,
    ) -> _MigrationBuilder:
        """Append a ``changeType`` directive: coerce ``field`` on ``table`` to the
        new type ``to`` via ``cast`` (one of :class:`Cast`). ``default``
        substitutes for un-coercible values; without it a single bad row rolls the
        whole migrate back atomically."""
        self._directives.append(
            _ChangeType(table=table, field=field, to=to, cast=cast, default=default)
        )
        return self

    def drop_field(self, table: str, field: str) -> _MigrationBuilder:
        """Append a ``dropField`` directive: remove ``field`` from ``table``'s schema."""
        self._directives.append(_DropField(table=table, field=field))
        return self

    def drop_table(self, name: str) -> _MigrationBuilder:
        """Append a ``dropTable`` directive: drop table ``name``."""
        self._directives.append(_DropTable(name=name))
        return self

    def drop_index(self, table: str, name: str) -> _MigrationBuilder:
        """Append a ``dropIndex`` directive: drop index ``name`` from ``table``."""
        self._directives.append(_DropIndex(table=table, name=name))
        return self

    def set_default(self, table: str, field: str, value: Any) -> _MigrationBuilder:
        """Append a ``setDefault`` directive: stamp ``field`` on ``table`` with
        ``value`` for rows that currently lack it."""
        self._directives.append(_SetDefault(table=table, field=field, value=value))
        return self

    def eval_expr(
        self,
        table: str,
        set: str,
        expr: str,
        where: str | None = None,
    ) -> _MigrationBuilder:
        """Append an ``evalExpr`` directive: evaluate the scoped raw-SQL ``expr``
        (one table's ``doc`` jsonb, no joins/DDL) and assign the result to ``set``.
        Optional ``where`` filters the target rows."""
        # ``model_validate`` for the same reason as ``rename_field`` — ``where``
        # is a Python keyword and can't appear as a constructor keyword arg.
        self._directives.append(
            _EvalExpr.model_validate({"table": table, "set": set, "expr": expr, "where": where})
        )
        return self

    def dry_run(self, dry_run: bool = True) -> _MigrationBuilder:
        """Set the ``dryRun`` flag so :meth:`build` produces a preview-only
        request (the server returns the report + derived schema without writing)."""
        self._dry_run = dry_run
        return self

    def build(self) -> MigrateRequest:
        """Materialize the :class:`MigrateRequest` from the accumulated directives."""
        return MigrateRequest(directives=list(self._directives), dry_run=self._dry_run)


class _MigrationNamespace:
    """Namespace for ``Migration`` so callers write ``Migration.builder()`` /
    ``Migration.model_validate(...)`` — mirrors ``rust-client``'s ``Migration``
    type surface and the existing :class:`Mutation` namespace."""

    builder = staticmethod(_MigrationBuilder)
    model_validate = staticmethod(MigrateRequest.model_validate)


Migration = _MigrationNamespace


# Resolve deferred annotations (``from __future__ import annotations`` makes
# every annotation a string; ``model_rebuild`` evaluates them so the
# discriminated-union ``Directive`` schema is fully built before first use).
MigrateRequest.model_rebuild()
