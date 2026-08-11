"""Declarative schema migration: ``Cast``, ``ValueExpr``, ``Directive`` (8 ops),
``MigrateRequest``, and the ``Migration`` builder.

Mirrors ``server/src/migrate.rs`` (the ``Directive`` enum + ``Cast`` +
``ValueExpr`` / ``CaseWhen`` + ``MigrateRequest``) and the builder ergonomics
of ``ts-client/src/migration.ts`` / ``rust-client/src/migration.rs``.

Wire shapes (load-bearing — match the server exactly):

* ``Directive`` is tagged by ``op`` (camelCase variants: ``renameField`` /
  ``renameTable`` / ``changeType`` / ``dropField`` / ``dropTable`` /
  ``dropIndex`` / ``setDefault`` / ``evalExpr``) with ``deny_unknown_fields``
  mirrored via ``extra="forbid"`` — the same shape contract as ``mutation.Step``.
* ``Cast`` is the closed set of sound coercions for ``changeType``
  (``toString`` / ``toNumber`` / ``toInt64`` / ``toBoolean``); it is also the
  coercion set for ``ValueExpr.cast``.
* ``ValueExpr`` is a closed, typed expression grammar for ``evalExpr.expr``
  (ENH-020 Stage 1, closing SEC-107). Tagged by ``op`` (camelCase:
  ``field`` / ``literal`` / ``concat`` / ``add`` / ``sub`` / ``mul`` / ``div``
  / ``coalesce`` / ``lower`` / ``upper`` / ``trim`` / ``cast`` / ``now`` /
  ``case``), ``deny_unknown_fields`` via ``extra="forbid"``. Mirrors
  ``server/src/migrate.rs::ValueExpr`` and ``FilterExpr``'s serde conventions.
  The grammar is closed (no subquery / function-by-name / raw-SQL node) so the
  SEC-107 injection concern cannot arise from a ``ValueExpr`` payload.
* ``CaseWhen`` is one branch of ``ValueExpr.case``: ``{when: FilterExpr,
  then: ValueExpr}`` (camelCase, ``extra="forbid"``).
* ``MigrateRequest`` is ``{"directives": Directive[], "dryRun": bool}``;
  ``dryRun`` defaults to ``False`` (server's ``#[serde(default)]``).
* ``evalExpr`` is dual-accept (ENH-020): ``expr`` is EITHER a ``ValueExpr``
  (typed safe path) OR a legacy raw-SQL ``str`` (deprecated, gated to the
  root admin_key — the SEC-107 boundary). ``where`` is EITHER a ``FilterExpr``
  OR a legacy raw-SQL ``str``. The two sources may not mix (a typed ``expr``
  requires a typed ``where``, and vice versa) — enforced server-side.
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
from .wire import FilterExpr, _Camel


class Cast(StrEnum):
    """Closed set of sound coercions for ``Directive.changeType``.

    Mirrors ``server/src/migrate::Cast`` (camelCase on the wire). ``StrEnum``
    so pydantic v2 serializes the string value directly and
    ``Cast.TO_STRING == "toString"`` for ergonomic builder calls. Shared by
    ``changeType`` and ``ValueExpr.cast`` (the four scalar casts sound to
    backfill).
    """

    TO_STRING = "toString"
    TO_NUMBER = "toNumber"
    TO_INT64 = "toInt64"
    TO_BOOLEAN = "toBoolean"


# --- ValueExpr (discriminator "op", camelCase) ---
#
# Closed, typed expression grammar for ``Directive.EvalExpr.expr`` (ENH-020
# Stage 1, closing SEC-107). Mirrors ``server/src/migrate.rs::ValueExpr``
# byte-for-byte: ``tag = "op"``, camelCase, ``deny_unknown_fields`` (via
# ``extra="forbid"`` on ``_Camel``) — the same serde conventions as
# ``wire.FilterExpr``. Every ``literal`` compiles to a bound ``$n`` placeholder
# (as jsonb); every ``field`` resolves through the table's ``TableDef``. The
# grammar is closed (no subquery / function-call-by-name / raw-SQL node) so the
# SEC-107 injection concern cannot arise from a ``ValueExpr`` payload.
#
# Self-referential union pattern mirrors ``wire.FilterExpr``: ``from __future__
# import annotations`` makes every annotation a string, the union references
# the variant classes by name, and ``model_rebuild()`` at module foot resolves
# the forward refs into a fully-built schema.


class _ValueField(_Camel):
    op: Literal["field"] = "field"
    field: str


class _ValueLiteral(_Camel):
    op: Literal["literal"] = "literal"
    value: Any


class _ValueConcat(_Camel):
    op: Literal["concat"] = "concat"
    parts: list[ValueExpr]


class _ValueAdd(_Camel):
    op: Literal["add"] = "add"
    left: ValueExpr
    right: ValueExpr


class _ValueSub(_Camel):
    op: Literal["sub"] = "sub"
    left: ValueExpr
    right: ValueExpr


class _ValueMul(_Camel):
    op: Literal["mul"] = "mul"
    left: ValueExpr
    right: ValueExpr


class _ValueDiv(_Camel):
    op: Literal["div"] = "div"
    left: ValueExpr
    right: ValueExpr


class _ValueCoalesce(_Camel):
    op: Literal["coalesce"] = "coalesce"
    parts: list[ValueExpr]


class _ValueLower(_Camel):
    op: Literal["lower"] = "lower"
    value: ValueExpr


class _ValueUpper(_Camel):
    op: Literal["upper"] = "upper"
    value: ValueExpr


class _ValueTrim(_Camel):
    op: Literal["trim"] = "trim"
    value: ValueExpr


class _ValueCast(_Camel):
    op: Literal["cast"] = "cast"
    value: ValueExpr
    to: Cast


class _ValueNow(_Camel):
    op: Literal["now"] = "now"


class _ValueCase(_Camel):
    op: Literal["case"] = "case"
    whens: list[CaseWhen]
    otherwise: ValueExpr


class CaseWhen(_Camel):
    """One branch of ``ValueExpr.case``. Wire shape ``{when, then}``.

    Mirrors ``server/src/migrate.rs::CaseWhen`` (camelCase,
    ``deny_unknown_fields`` via ``extra="forbid"`` on ``_Camel``). ``when`` is
    the read-path ``FilterExpr`` (field references schema-validated, values
    bound); ``then`` is the typed expression result on a match.
    """

    when: FilterExpr
    then: ValueExpr


#: Discriminated union of all 14 ``ValueExpr`` ops. The ``op`` literal drives
#: dispatch; ``deny_unknown_fields`` is per-variant via ``extra="forbid"`` on
#: ``_Camel`` — the same shape contract as ``wire.FilterExpr``.
ValueExpr = Annotated[
    (
        _ValueField
        | _ValueLiteral
        | _ValueConcat
        | _ValueAdd
        | _ValueSub
        | _ValueMul
        | _ValueDiv
        | _ValueCoalesce
        | _ValueLower
        | _ValueUpper
        | _ValueTrim
        | _ValueCast
        | _ValueNow
        | _ValueCase
    ),
    Field(discriminator="op"),
]


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
    """``evalExpr`` directive (ENH-020 dual-accept).

    ``expr`` is EITHER a typed :data:`ValueExpr` (the safe path — closed
    grammar, all literals bound, SEC-107 structural close) OR a legacy raw-SQL
    ``str`` (deprecated, gated to the root admin_key until the string form is
    removed). ``where_`` is EITHER a typed :data:`FilterExpr` OR a legacy
    raw-SQL predicate ``str``. The two sources may not mix — a typed ``expr``
    requires a typed ``where_`` (and vice versa); enforced server-side. The
    untagged-object-vs-string union mirrors the server's ``ExprSource`` /
    ``CondSource`` ``#[serde(untagged)]`` (object arm first; a bare string
    fails the model parse and is taken as legacy).
    """

    op: Literal["evalExpr"] = "evalExpr"
    table: str
    set: str
    expr: ValueExpr | str
    # `where` is a Python keyword — field is `where_`, wire alias is explicit.
    where_: FilterExpr | str | None = Field(default=None, alias="where")

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
        """Append a legacy raw-SQL ``evalExpr`` directive (ENH-020 / SEC-107 —
        deprecated). Evaluate the scoped raw-SQL ``expr`` (one table's ``doc``
        jsonb, no joins/DDL) and assign the result to ``set``. Optional
        ``where`` filters the target rows.

        Prefer :meth:`eval_expr_typed` — the typed :class:`ValueExpr` path is
        the safe form (closed grammar, all literals bound, no injection
        surface). This legacy string form remains gated to the root
        ``admin_key`` server-side; the two sources may not mix."""
        # ``model_validate`` for the same reason as ``rename_field`` — ``where``
        # is a Python keyword and can't appear as a constructor keyword arg.
        self._directives.append(
            _EvalExpr.model_validate({"table": table, "set": set, "expr": expr, "where": where})
        )
        return self

    def eval_expr_typed(
        self,
        table: str,
        set: str,
        expr: ValueExpr,
        where: FilterExpr | None = None,
    ) -> _MigrationBuilder:
        """Append a typed ``evalExpr`` directive (ENH-020, SEC-107 structural
        close). The safe path: ``expr`` is a closed :class:`ValueExpr` grammar
        and ``where`` is an optional typed :class:`FilterExpr`. The two sources
        may not mix — pass both typed, or use :meth:`eval_expr` for the legacy
        raw-SQL form (never combine a typed ``expr`` with a legacy ``where``,
        or vice versa)."""
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
# discriminated-union ``Directive`` and ``ValueExpr`` schemas are fully built
# before first use). ``ValueExpr`` is mutually recursive with ``CaseWhen``
# (``_ValueCase`` -> ``CaseWhen`` -> ``ValueExpr``); rebuilding ``CaseWhen``
# first ensures both arms of the cycle resolve, then ``MigrateRequest``
# cascades through ``Directive`` -> ``_EvalExpr`` -> ``ValueExpr``.
CaseWhen.model_rebuild()
MigrateRequest.model_rebuild()
