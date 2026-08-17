"""Filter validation and evaluation for the in-memory harness (mirrors
``rust-client/src/in_memory/validate.rs``): structural validation of the
query DSL's ``FilterExpr`` plus its per-row evaluator and comparison
domains."""

from __future__ import annotations

import json
import math
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


def _eval_filter_expr(expr: FilterExpr, doc: dict[str, Any]) -> bool:
    """Evaluate a ``FilterExpr`` predicate against a stored doc. A null/absent
    field never matches (SQL NULL exclusion). Assumes ``_validate_filter`` passed."""
    match expr:
        case _FilterAnd(exprs=exprs):
            return all(_eval_filter_expr(e, doc) for e in exprs)
        case _FilterOr(exprs=exprs):
            return any(_eval_filter_expr(e, doc) for e in exprs)
        case _FilterIn(field=fld, values=values):
            return any(_compare_leaf("eq", fld, v, doc) for v in values)
        case (
            _FilterEq(field=fld, value=val)
            | _FilterNeq(field=fld, value=val)
            | _FilterGt(field=fld, value=val)
            | _FilterGte(field=fld, value=val)
            | _FilterLt(field=fld, value=val)
            | _FilterLte(field=fld, value=val)
        ):
            return _compare_leaf(expr.op, fld, val, doc)
        case _FilterNot(expr=inner):
            return not _eval_filter_expr(inner, doc)
        case _FilterContains(field=fld, value=val):
            arr = doc.get(fld)
            want = json.dumps(val, sort_keys=True)
            return isinstance(arr, list) and any(json.dumps(v, sort_keys=True) == want for v in arr)
        case _FilterExists(field=fld):
            return doc.get(fld) is not None
        case _:
            return False


def _compare_leaf(op: str, field: str, filter_value: Any, doc: dict[str, Any]) -> bool:
    doc_val = doc.get(field)
    if doc_val is None:
        return False
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
