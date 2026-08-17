"""Filter validation and evaluation for the in-memory harness (mirrors
``rust-client/src/in_memory/validate.rs``): structural validation of the
query DSL's ``FilterExpr`` plus its per-row evaluator and comparison
domains."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from typing import Any

from ..errors import ErrorCode, RtDbError
from ..wire import (
    FilterExpr,
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


def _eval_filter_expr(expr: FilterExpr, doc: dict[str, Any], fields: Mapping[str, Any]) -> bool:
    """Evaluate a ``FilterExpr`` predicate against a stored doc. The filter
    value's kind picks the comparison domain — string compares the doc field's
    ``->>`` text, number compares it as ``float8``, boolean as ``boolean`` —
    EXCEPT on a declared ``int64`` field, where a string value (the wire form
    the server types as a ``bigint`` bind) compares numerically: decimal
    strings must order ``-605 < -1 < 15``, not lexicographically (ENH-027
    parity fix). A null/absent field never matches (SQL NULL exclusion).
    ``fields`` is the table's declared field map (pass an empty mapping for
    type-less evaluation, e.g. unit tests). Assumes ``_validate_filter`` passed."""
    match expr:
        case _FilterAnd(exprs=exprs):
            return all(_eval_filter_expr(e, doc, fields) for e in exprs)
        case _FilterOr(exprs=exprs):
            return any(_eval_filter_expr(e, doc, fields) for e in exprs)
        case _FilterIn(field=fld, values=values):
            return any(_compare_leaf("eq", fld, v, doc, fields) for v in values)
        case (
            _FilterEq(field=fld, value=val)
            | _FilterNeq(field=fld, value=val)
            | _FilterGt(field=fld, value=val)
            | _FilterGte(field=fld, value=val)
            | _FilterLt(field=fld, value=val)
            | _FilterLte(field=fld, value=val)
        ):
            return _compare_leaf(expr.op, fld, val, doc, fields)
        case _FilterNot(expr=inner):
            return not _eval_filter_expr(inner, doc, fields)
        case _FilterContains(field=fld, value=val):
            arr = doc.get(fld)
            want = json.dumps(val, sort_keys=True)
            return isinstance(arr, list) and any(json.dumps(v, sort_keys=True) == want for v in arr)
        case _FilterExists(field=fld):
            return doc.get(fld) is not None
        case _:
            return False


def _compare_leaf(
    op: str, field: str, filter_value: Any, doc: dict[str, Any], fields: Mapping[str, Any]
) -> bool:
    doc_val = doc.get(field)
    if doc_val is None:
        return False
    if isinstance(filter_value, str) and _is_int64_field(fields.get(field)):
        # The server binds a string filter value on an int64 field as a typed
        # ``bigint`` against the typed column (indexed fields) and rejects it
        # on the jsonb path — so any legal comparison is numeric. Parse both
        # sides exactly as i64 (i64::MAX is not float-exact); an unparseable
        # value never matches.
        lhs = _parse_i64_exact(doc_val) if isinstance(doc_val, str) else None
        if lhs is None:
            return False
        rhs = _parse_i64_exact(filter_value)
        return False if rhs is None else _compare_values(op, lhs, rhs)
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


def _field_type_tag(ty: Any) -> Any:
    """The ``type`` discriminator of a declared field type — a pydantic
    ``FieldType`` instance (production, via ``TableDef.fields``) or the raw
    dict the schema builders emit."""
    if isinstance(ty, dict):
        return ty.get("type")
    return getattr(ty, "type", None)


def _is_int64_field(ty: Any) -> bool:
    """Whether a declared field type is ``int64`` (an ``optional<int64>``
    unwraps to it — mirrors the rust ``is_int64_field`` / the server's
    ``eq_bind_for`` Optional unwrap)."""
    if ty is None:
        return False
    tag = _field_type_tag(ty)
    if tag == "int64":
        return True
    if tag == "optional":
        inner = ty.get("inner") if isinstance(ty, dict) else getattr(ty, "inner", None)
        return _field_type_tag(inner) == "int64"
    return False


_I64_RE = re.compile(r"[+-]?\d+")


def _parse_i64_exact(s: str) -> int | None:
    """Exact ``i64::from_str`` mirror: an optional ``+``/``-`` sign then one
    or more ASCII digits, within the i64 range. Returns ``None`` when ``s`` is
    not a strict i64 decimal string (unlike store's ordering fallback
    ``_parse_i64``, which maps unparseable to ``i64::MIN``)."""
    if _I64_RE.fullmatch(s) is None:
        return None
    n = int(s)
    return n if -(2**63) <= n <= 2**63 - 1 else None


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
