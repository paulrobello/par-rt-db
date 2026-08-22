"""The closed ``ValueExpr`` grammar (ENH-020 / ENH-028) — the typed,
injection-safe expression language shared by migrate's ``evalExpr`` backfill
and computed fields.

Mirrors ``server/src/value_expr.rs`` (the canonical home of the wire types —
``migrate.rs`` re-exports them there, exactly as :mod:`par_rt_db.migration`
re-exports them here). One wire shape, two executions: the server's SQL
compiler for the one-shot migrate UPDATE, and the in-memory interpreter
(:mod:`par_rt_db.in_memory.value_expr`) for per-write stamping in the engine.

Wire shapes (load-bearing — match the server exactly):

* ``Cast`` is the closed set of sound coercions shared by
  ``changeType`` and ``ValueExpr.cast`` (``toString`` / ``toNumber`` /
  ``toInt64`` / ``toBoolean``; camelCase on the wire).
* ``ValueExpr`` is tagged by ``op`` (camelCase variants: ``field`` /
  ``literal`` / ``concat`` / ``add`` / ``sub`` / ``mul`` / ``div`` /
  ``coalesce`` / ``lower`` / ``upper`` / ``trim`` / ``cast`` / ``now`` /
  ``case``), ``deny_unknown_fields`` via ``extra="forbid"`` — the same serde
  conventions as ``wire.FilterExpr``. The grammar is closed (no subquery /
  function-by-name / raw-SQL node) so the SEC-107 injection concern cannot
  arise from a ``ValueExpr`` payload.
* ``CaseWhen`` is one branch of ``ValueExpr.case``: ``{when: FilterExpr,
  then: ValueExpr}`` (camelCase, ``extra="forbid"``).

The ``ve`` namespace is the DSL constructor surface (``ve.field("name")``,
``ve.concat(...)`` ...), mirroring ``schema.t`` for field types and the rust
client's ``ValueExpr::field``-style associated constructors.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import Field

from .wire import FilterExpr, _Camel


class Cast(StrEnum):
    """Closed set of sound coercions for ``Directive.changeType``.

    Mirrors ``server/src/value_expr.rs::Cast`` (camelCase on the wire).
    ``StrEnum`` so pydantic v2 serializes the string value directly and
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
# Stage 1, closing SEC-107) and ``TableDef.computed`` entries (ENH-028).
# Mirrors ``server/src/value_expr.rs::ValueExpr`` byte-for-byte:
# ``tag = "op"``, camelCase, ``deny_unknown_fields`` (via ``extra="forbid"``
# on ``_Camel``) — the same serde conventions as ``wire.FilterExpr``. Every
# ``literal`` compiles to a bound ``$n`` placeholder (as jsonb); every
# ``field`` resolves through the table's ``TableDef``.
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

    Mirrors ``server/src/value_expr.rs::CaseWhen`` (camelCase,
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

# Resolve deferred annotations (``from __future__ import annotations`` makes
# every annotation a string; ``model_rebuild`` evaluates them so the
# discriminated-union schema is fully built before first use). ``ValueExpr``
# is mutually recursive with ``CaseWhen`` (``_ValueCase`` -> ``CaseWhen`` ->
# ``ValueExpr``); rebuilding ``CaseWhen`` first ensures both arms of the cycle
# resolve.
CaseWhen.model_rebuild()


# --- ``ve`` expression constructors ---
#
# Constructors return the wire-identical dict (the simplest form, mirroring
# ``schema.t``); ``SchemaDef.model_validate`` routes each through the
# discriminated ``ValueExpr`` union at ``build()`` time. ``case`` accepts
# ``CaseWhen`` models or plain ``{"when": ..., "then": ...}`` dicts.


class _VeNamespace:
    """Expression constructors (``ve.field(name)`` ... ``ve.case(...)``)."""

    @staticmethod
    def field(name: str) -> dict[str, Any]:
        """Read declared field ``name`` as text (the ``doc->>'field'`` read)."""
        return {"op": "field", "field": name}

    @staticmethod
    def literal(value: Any) -> dict[str, Any]:
        """A constant JSON value (string/number/bool/object/array/null)."""
        return {"op": "literal", "value": value}

    @staticmethod
    def concat(*parts: Any) -> dict[str, Any]:
        """Concatenate the text form of ``parts``, skipping nulls (Postgres
        ``concat()`` — wrap operands in :meth:`coalesce` for explicit control)."""
        return {"op": "concat", "parts": list(parts)}

    @staticmethod
    def add(left: Any, right: Any) -> dict[str, Any]:
        """``left + right`` as IEEE doubles; a null operand nulls the result."""
        return {"op": "add", "left": left, "right": right}

    @staticmethod
    def sub(left: Any, right: Any) -> dict[str, Any]:
        """``left - right`` as IEEE doubles; a null operand nulls the result."""
        return {"op": "sub", "left": left, "right": right}

    @staticmethod
    def mul(left: Any, right: Any) -> dict[str, Any]:
        """``left * right`` as IEEE doubles; a null operand nulls the result."""
        return {"op": "mul", "left": left, "right": right}

    @staticmethod
    def div(left: Any, right: Any) -> dict[str, Any]:
        """``left / right`` as IEEE doubles; a null operand nulls the result
        (null propagation precedes the division-by-zero error)."""
        return {"op": "div", "left": left, "right": right}

    @staticmethod
    def coalesce(*parts: Any) -> dict[str, Any]:
        """First non-null part, else null."""
        return {"op": "coalesce", "parts": list(parts)}

    @staticmethod
    def lower(value: Any) -> dict[str, Any]:
        """Lowercase the text form of ``value`` (null passes through)."""
        return {"op": "lower", "value": value}

    @staticmethod
    def upper(value: Any) -> dict[str, Any]:
        """Uppercase the text form of ``value`` (null passes through)."""
        return {"op": "upper", "value": value}

    @staticmethod
    def trim(value: Any) -> dict[str, Any]:
        """Strip leading/trailing SPACES ONLY from the text form of ``value``
        (Postgres ``btrim`` default — not full Unicode whitespace)."""
        return {"op": "trim", "value": value}

    @staticmethod
    def cast(value: Any, to: Cast) -> dict[str, Any]:
        """Apply the closed scalar coercion ``to`` (one of :class:`Cast`)."""
        return {"op": "cast", "value": value, "to": to}

    @staticmethod
    def now() -> dict[str, Any]:
        """Current epoch-milliseconds as a JSON number (the same value the
        engine stamps for ``updatedAtField``)."""
        return {"op": "now"}

    @staticmethod
    def case(whens: list[Any], otherwise: Any) -> dict[str, Any]:
        """First matching ``when``'s ``then``, else ``otherwise``. Each
        ``whens`` entry is a :class:`CaseWhen` (or the plain
        ``{"when": ..., "then": ...}`` dict)."""
        return {"op": "case", "whens": list(whens), "otherwise": otherwise}


ve = _VeNamespace()
