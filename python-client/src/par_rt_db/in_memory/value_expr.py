"""Computed-field machinery for the in-memory harness (ENH-028): the
``ValueExpr`` interpreter (:func:`eval_value_expr` — the per-write counterpart
of the server's SQL compiler), the field walkers shared by push validation and
migrate's rename/drop rules, the six-rule push validation
(:func:`_validate_computed`, a port of ``server/src/schema.rs::validate_computed``),
and the write-path stamp (:func:`_stamp_computed`, port of
``server/src/txn.rs::stamp_computed``).

Interpreter semantics are pinned by the computed-fields plan's "ValueExpr
interpreter semantics" table, authoritative for the server and all four client
engines: field reads are text extraction (mirroring ``doc->>'field'`` —
numbers/bools/objects render as their compact JSON text), arithmetic is IEEE
doubles with SQL-NULL propagation BEFORE the divide-by-zero check, ``trim``
strips spaces only (Postgres ``btrim`` default), ``now()`` is epoch-ms, and
``Case.whens`` evaluate through the engine's own filter matcher
(:func:`par_rt_db.in_memory.validate._eval_filter_expr` — principal markers
are push-rejected inside computed exprs, so no principal context is needed).
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable
from copy import deepcopy
from typing import Any

from ..errors import ErrorCode, RtDbError
from ..schema import SchemaDef, TableDef, _FInt64, _FOptional
from ..value_expr import (
    Cast,
    ValueExpr,
    _ValueAdd,
    _ValueCase,
    _ValueCast,
    _ValueCoalesce,
    _ValueConcat,
    _ValueDiv,
    _ValueField,
    _ValueLiteral,
    _ValueLower,
    _ValueMul,
    _ValueNow,
    _ValueSub,
    _ValueTrim,
    _ValueUpper,
)
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
from .validate import _eval_filter_expr, _parse_i64_exact

_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1

#: Postgres's boolean-literal word set (case-insensitive) — the strings
#: ``Cast.toBoolean`` accepts (mirrors server ``value_expr.rs``).
_BOOLEAN_TRUE_WORDS = frozenset(("true", "t", "yes", "on", "1"))
_BOOLEAN_FALSE_WORDS = frozenset(("false", "f", "no", "off", "0"))

#: Strict decimal grammar for ``to_numeric``'s string parse — Python's
#: ``float()`` accepts forms Rust's ``f64::from_str`` rejects (PEP 515
#: underscores like ``"1_0"``, hex, a bare sign), so the grammar is checked
#: first (mirrors the ts engine's ``parseNumericString``).
_NUMERIC_RE = re.compile(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?")


def to_text(v: Any) -> str | None:
    """JSON value → text, mirroring the SQL ``doc->>'field'`` extraction the
    compile path emits. ``None`` means SQL NULL (JSON ``null``) — only a JSON
    null maps to ``None``. Numbers use their JSON number text form (``42`` →
    ``"42"``, ``42.5`` → ``"42.5"``, ``43.0`` → ``"43.0"`` — the serde_json
    float rendering); objects/arrays use compact JSON text (``{"a":1}`` —
    deliberately not Postgres's spaced jsonb text; the semantics table pins
    this convention for all five implementations)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return json.dumps(v)
    if isinstance(v, str):
        return v
    return json.dumps(v, separators=(",", ":"))


def to_numeric(v: Any) -> float | None:
    """JSON value → float for the arithmetic nodes. ``None`` means SQL NULL
    (JSON ``null`` — propagation, not an error). Numbers yield their float;
    strings are trimmed and strictly parsed (the whole string must be the
    number); bool/object/array are type errors."""
    if v is None:
        return None
    if isinstance(v, bool):
        raise RtDbError(ErrorCode.BAD_REQUEST, "cannot cast to number")
    if isinstance(v, float):
        return v
    if isinstance(v, int):
        try:
            return float(v)
        except OverflowError:  # beyond f64 — the server's as_f64 None arm
            raise RtDbError(ErrorCode.BAD_REQUEST, "cannot cast to number") from None
    if isinstance(v, str):
        t = v.strip()
        if _NUMERIC_RE.fullmatch(t) is None:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"cannot cast {v!r} to number")
        return float(t)
    raise RtDbError(ErrorCode.BAD_REQUEST, "cannot cast to number")


def _finite_number(x: float) -> float:
    """IEEE double → JSON number. A non-finite result (NaN, ±inf —
    overflow-shaped arithmetic) is an error rather than a stored value,
    mirroring ``serde_json::Number::from_f64`` being ``None`` exactly there."""
    if not math.isfinite(x):
        raise RtDbError(ErrorCode.BAD_REQUEST, "numeric result is not finite")
    return x


def _cast_to_int64(v: Any) -> Any:
    """``Cast.toInt64`` — a JSON int must sit in the i64 range (a float payload
    like ``3.0`` is NOT integral per serde_json's ``as_i64`` and errors, same
    as the server); a string is trimmed and strictly parsed as i64. The result
    is a JSON number (int); the int64 *string* wire convention applies only to
    stored int64 fields (the plan's "Int64 note")."""
    if v is None:
        return None
    if isinstance(v, bool):
        raise RtDbError(ErrorCode.BAD_REQUEST, "cannot cast to int64")
    if isinstance(v, int):
        if not _I64_MIN <= v <= _I64_MAX:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"cannot cast {v} to int64")
        return v
    if isinstance(v, float):
        raise RtDbError(ErrorCode.BAD_REQUEST, f"cannot cast {v} to int64")
    if isinstance(v, str):
        parsed = _parse_i64_exact(v.strip())
        if parsed is None:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"cannot cast {v!r} to int64")
        return parsed
    raise RtDbError(ErrorCode.BAD_REQUEST, "cannot cast to int64")


def _cast_to_boolean(v: Any) -> Any:
    """``Cast.toBoolean`` — bools pass through; numbers accept exactly ``1``/
    ``0`` (numeric equality, so ``1.0``/``0.0`` agree); strings match
    case-insensitively against Postgres's boolean literal set."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, float | int):
        if v == 1.0:
            return True
        if v == 0.0:
            return False
        raise RtDbError(ErrorCode.BAD_REQUEST, f"cannot cast {v} to boolean")
    if isinstance(v, str):
        lowered = v.lower()
        if lowered in _BOOLEAN_TRUE_WORDS:
            return True
        if lowered in _BOOLEAN_FALSE_WORDS:
            return False
        raise RtDbError(ErrorCode.BAD_REQUEST, f"cannot cast {v!r} to boolean")
    raise RtDbError(ErrorCode.BAD_REQUEST, "cannot cast to boolean")


def eval_value_expr(
    ve: ValueExpr,
    doc: dict[str, Any],
    now_ms: int,
    fields: Any,
) -> Any:
    """In-memory interpreter for :class:`ValueExpr` — the per-write counterpart
    of the server's SQL compiler, used by computed-field stamping. Field reads
    are text extraction, arithmetic is IEEE doubles with SQL-NULL propagation,
    and a non-finite result is an error. ``Case`` predicates reuse the engine's
    filter matcher over ``fields`` (the table's declared field map); principal
    markers are push-rejected inside computed exprs, so no principal context is
    needed. Returns a JSON value (``None`` for a null result)."""
    match ve:
        case _ValueField(field=name):
            return to_text(doc.get(name))
        case _ValueLiteral(value=v):
            return deepcopy(v)
        case _ValueConcat(parts=parts):
            out: list[str] = []
            for p in parts:
                # to_text is None exactly for null parts — Postgres concat()
                # skips them rather than nulling the result.
                text = to_text(eval_value_expr(p, doc, now_ms, fields))
                if text is not None:
                    out.append(text)
            return "".join(out)
        case (
            _ValueAdd(left=left, right=right)
            | _ValueSub(left=left, right=right)
            | _ValueMul(left=left, right=right)
            | _ValueDiv(left=left, right=right)
        ):
            lhs = to_numeric(eval_value_expr(left, doc, now_ms, fields))
            rhs = to_numeric(eval_value_expr(right, doc, now_ms, fields))
            if lhs is None or rhs is None:
                # Either operand SQL-NULL → NULL; propagation precedes the
                # zero-divisor and finiteness checks (null / 0 is null).
                return None
            if isinstance(ve, _ValueDiv) and rhs == 0.0:
                # `rhs == 0.0` is true for -0.0 too (IEEE equality), so both zero
                # spellings are the same divisor error.
                raise RtDbError(ErrorCode.BAD_REQUEST, "division by zero")
            if isinstance(ve, _ValueAdd):
                x = lhs + rhs
            elif isinstance(ve, _ValueSub):
                x = lhs - rhs
            elif isinstance(ve, _ValueMul):
                x = lhs * rhs
            else:
                x = lhs / rhs
            return _finite_number(x)
        case _ValueCoalesce(parts=parts):
            for p in parts:
                v = eval_value_expr(p, doc, now_ms, fields)
                if v is not None:
                    return v
            return None
        case _ValueLower(value=inner):
            text = to_text(eval_value_expr(inner, doc, now_ms, fields))
            return None if text is None else text.lower()
        case _ValueUpper(value=inner):
            text = to_text(eval_value_expr(inner, doc, now_ms, fields))
            return None if text is None else text.upper()
        case _ValueTrim(value=inner):
            text = to_text(eval_value_expr(inner, doc, now_ms, fields))
            # Spaces only — Postgres btrim's default, not Unicode whitespace: a
            # leading tab survives.
            return None if text is None else text.strip(" ")
        case _ValueCast(value=inner, to=to):
            v = eval_value_expr(inner, doc, now_ms, fields)
            if to == Cast.TO_STRING:
                return to_text(v)
            if to == Cast.TO_NUMBER:
                x = to_numeric(v)
                return None if x is None else _finite_number(x)
            if to == Cast.TO_INT64:
                return _cast_to_int64(v)
            return _cast_to_boolean(v)
        case _ValueNow():
            return now_ms
        case _ValueCase(whens=whens, otherwise=otherwise):
            for cw in whens:
                if _eval_filter_expr(cw.when, doc, fields):
                    return eval_value_expr(cw.then, doc, now_ms, fields)
            return eval_value_expr(otherwise, doc, now_ms, fields)
        case _:
            raise RtDbError(ErrorCode.INTERNAL, "unknown value expr op")


def walk_value_expr_fields(ve: ValueExpr, f: Callable[[str], None]) -> None:
    """Visit every field name a ``ValueExpr`` reads: each ``field`` node, every
    ``case`` branch's ``then``/``otherwise``, and every ``FilterExpr`` field
    inside ``case.whens`` — the same field set push validation type-checks
    (port of server ``value_expr.rs::walk_value_expr_fields``)."""
    match ve:
        case _ValueField(field=name):
            f(name)
        case _ValueConcat(parts=parts) | _ValueCoalesce(parts=parts):
            for p in parts:
                walk_value_expr_fields(p, f)
        case (
            _ValueAdd(left=left, right=right)
            | _ValueSub(left=left, right=right)
            | _ValueMul(left=left, right=right)
            | _ValueDiv(left=left, right=right)
        ):
            walk_value_expr_fields(left, f)
            walk_value_expr_fields(right, f)
        case (
            _ValueLower(value=inner)
            | _ValueUpper(value=inner)
            | _ValueTrim(value=inner)
            | _ValueCast(value=inner)
        ):
            walk_value_expr_fields(inner, f)
        case _ValueCase(whens=whens, otherwise=otherwise):
            for cw in whens:
                walk_filter_expr_fields(cw.when, f)
                walk_value_expr_fields(cw.then, f)
            walk_value_expr_fields(otherwise, f)
        case _ValueLiteral() | _ValueNow():
            pass


def walk_filter_expr_fields(expr: FilterExpr, f: Callable[[str], None]) -> None:
    """The ``FilterExpr`` half of the walk: ``and``/``or``/``not`` recurse;
    every leaf variant carries ``field`` (port of server
    ``value_expr.rs::walk_filter_expr_fields``)."""
    match expr:
        case (
            _FilterEq(field=name)
            | _FilterNeq(field=name)
            | _FilterGt(field=name)
            | _FilterGte(field=name)
            | _FilterLt(field=name)
            | _FilterLte(field=name)
            | _FilterIn(field=name)
            | _FilterContains(field=name)
            | _FilterExists(field=name)
        ):
            f(name)
        case _FilterAnd(exprs=exprs) | _FilterOr(exprs=exprs):
            for e in exprs:
                walk_filter_expr_fields(e, f)
        case _FilterNot(expr=inner):
            walk_filter_expr_fields(inner, f)


def _stamp_computed(table_def: TableDef, doc: dict[str, Any], now: int) -> dict[str, Any]:
    """Stamp the table's computed fields (port of server
    ``txn.rs::stamp_computed``): every ``computed`` entry is re-evaluated
    against the final doc and stored — a null result REMOVES the key (an unset
    optional field is an absent key, ``strip_unset_optionals``' shape
    convention) and a non-null result overwrites whatever is there (the
    ``ownerField`` authority model: client-supplied values never survive). An
    evaluation error fails the whole write as ``BAD_REQUEST``, naming the
    field. Runs last in the stamp chain and before ``validate_doc`` at every
    site. Returns a NEW dict; the incoming doc is never mutated."""
    if not table_def.computed:
        return doc
    out = dict(doc)
    for name, expr in table_def.computed.items():
        try:
            value = eval_value_expr(expr, out, now, table_def.fields)
        except RtDbError as err:
            raise RtDbError(
                ErrorCode.BAD_REQUEST, f"computed field '{name}': {err.message}"
            ) from err
        if value is None:
            out.pop(name, None)
        else:
            out[name] = value
    return out


# --- Push validation (port of server schema.rs::validate_computed) ---

#: Sample value per statically-known result kind, checked against the field's
#: declared type with the engine's own value validator.
_STATIC_KIND_SAMPLE: dict[str, Any] = {"string": "s", "number": 1, "boolean": True}
_STATIC_KIND_STR = {"string": "a string", "number": "a number", "boolean": "a boolean"}


def _infer_static_kind(ve: ValueExpr) -> str | None:
    """The statically-known result kind of a ``ValueExpr`` for the computed-
    field push check. ``None`` means the result kind varies by input —
    ``field`` (text extraction of any JSON value), ``coalesce``/``case``
    (whichever branch wins), and the null/object/array literals whose runtime
    ``validate_doc`` check is the only guard (port of server
    ``schema.rs::infer_static_kind``)."""
    match ve:
        case _ValueCast(to=to):
            if to == Cast.TO_STRING:
                return "string"
            if to == Cast.TO_BOOLEAN:
                return "boolean"
            return "number"  # toNumber / toInt64
        case _ValueConcat() | _ValueLower() | _ValueUpper() | _ValueTrim():
            return "string"
        case _ValueAdd() | _ValueSub() | _ValueMul() | _ValueDiv() | _ValueNow():
            return "number"
        case _ValueLiteral(value=v):
            if isinstance(v, str):
                return "string"
            if isinstance(v, bool):
                return "boolean"
            if isinstance(v, float | int):
                return "number"
            return None  # null / object / array literal
        case _:
            return None  # field / coalesce / case


def _is_principal_marker(v: Any) -> bool:
    """``True`` iff ``v`` is a principal marker — ``{"$user": true}`` or
    ``{"$email": true}`` (port of server ``schema.rs::is_principal_marker``)."""
    if isinstance(v, dict) and len(v) == 1:
        return v.get("$user") is True or v.get("$email") is True
    return False


def _validate_computed_case_whens(ve: ValueExpr, table: TableDef) -> None:
    """Walk a computed expression's ``case`` nodes validating each ``when``
    filter with the marker-rejecting rule (server rule 4: computed exprs run
    on every write with no interactive principal, so a ``$user``/``$email``
    marker has no value to resolve); ``then``/``otherwise`` recurse so a
    ``case`` nested inside a branch is covered. Declared-field checks for the
    same filters come from :func:`walk_value_expr_fields` (rule 3)."""
    match ve:
        case _ValueCase(whens=whens, otherwise=otherwise):
            for cw in whens:
                _reject_principal_markers(cw.when)
                _validate_computed_case_whens(cw.then, table)
            _validate_computed_case_whens(otherwise, table)
        case _ValueConcat(parts=parts) | _ValueCoalesce(parts=parts):
            for p in parts:
                _validate_computed_case_whens(p, table)
        case (
            _ValueAdd(left=left, right=right)
            | _ValueSub(left=left, right=right)
            | _ValueMul(left=left, right=right)
            | _ValueDiv(left=left, right=right)
        ):
            _validate_computed_case_whens(left, table)
            _validate_computed_case_whens(right, table)
        case (
            _ValueLower(value=inner)
            | _ValueUpper(value=inner)
            | _ValueTrim(value=inner)
            | _ValueCast(value=inner)
        ):
            _validate_computed_case_whens(inner, table)
        case _ValueField() | _ValueLiteral() | _ValueNow():
            pass


def _reject_principal_markers(expr: FilterExpr) -> None:
    """Reject ``{"$user": true}`` / ``{"$email": true}`` marker values in any
    value position of a ``case.when`` filter (the marker-rejecting mode of the
    server's ``validate_filter_expr_fields``)."""
    match expr:
        case (
            _FilterEq(field=fld, value=val)
            | _FilterNeq(field=fld, value=val)
            | _FilterGt(field=fld, value=val)
            | _FilterGte(field=fld, value=val)
            | _FilterLt(field=fld, value=val)
            | _FilterLte(field=fld, value=val)
            | _FilterContains(field=fld, value=val)
        ):
            if _is_principal_marker(val):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    'principal markers ({"$user":true}/{"$email":true}) are not'
                    f" allowed in client filters (field '{fld}')",
                )
        case _FilterIn(field=fld, values=values):
            for v in values:
                if _is_principal_marker(v):
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        'principal markers ({"$user":true}/{"$email":true}) are not'
                        f" allowed in client filters (field '{fld}')",
                    )
        case _FilterAnd(exprs=exprs) | _FilterOr(exprs=exprs):
            for e in exprs:
                _reject_principal_markers(e)
        case _FilterNot(expr=inner):
            _reject_principal_markers(inner)
        case _FilterExists():
            pass


def _validate_computed(schema: SchemaDef) -> None:
    """Computed-field push validation (port of server
    ``schema.rs::validate_computed`` — same rules, same order, same
    ``BAD_REQUEST`` semantics). Per table:

    1. every ``computed`` key names a declared field;
    2. the key is not one of the server-stamped declaration fields
       (``ownerField``/``collaboratorsField``/``autoIncrementField``);
    3. every field the expression references (including ``case.when`` filter
       fields) is declared and not itself computed (no chained or cyclic
       evaluation);
    4. ``case.when`` filters reject principal markers;
    5. when the expression's result kind is statically known, the field's type
       must accept a value of that kind (``validate_value`` is the wire
       contract, but int64's wire form is a decimal STRING: a Number-kind
       result can never validate, while a String-kind one can — decimal-ness
       stays a runtime ``validate_doc`` check);
    6. the table's ``authorize`` predicate references no computed field (on
       insert the authorize check runs before computed stamping, so such a
       predicate would evaluate forgeable client input).
    """
    # Deferred import: ``store`` imports this module at load time (for the
    # stamp/push hooks) and ``validate_value`` lives there.
    from .store import validate_value

    for table_name, table in schema.tables.items():
        for field, expr in table.computed.items():
            if field not in table.fields:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"computed field '{table_name}.{field}' is not a declared field",
                )
            if table.owner_field == field:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"computed field '{table_name}.{field}' must not be the table's ownerField",
                )
            if table.collaborators_field == field:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"computed field '{table_name}.{field}' must not be the table's"
                    " collaboratorsField",
                )
            if table.auto_increment_field == field:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"computed field '{table_name}.{field}' must not be the table's"
                    " autoIncrementField",
                )
            # First offense in walk order wins; the walk covers `field` nodes
            # and every `case.when` filter field.
            referenced: list[str] = []
            walk_value_expr_fields(expr, referenced.append)
            for ref in referenced:
                if ref not in table.fields:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        f"computed field '{table_name}.{field}' references undeclared"
                        f" field '{ref}'",
                    )
                if ref in table.computed:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        f"computed field '{table_name}.{field}' references computed"
                        f" field '{ref}' (computed fields may not reference each other)",
                    )
            _validate_computed_case_whens(expr, table)
            kind = _infer_static_kind(expr)
            if kind is not None:
                sample = _STATIC_KIND_SAMPLE[kind]
                # Optional unwrapping admits the nullable spelling; an int64
                # field additionally accepts a String-kind result (the decimal
                # string wire convention).
                inner = table.fields[field]
                while isinstance(inner, _FOptional):
                    inner = inner.inner
                accepts = validate_value(table.fields[field], sample) or (
                    isinstance(inner, _FInt64) and kind == "string"
                )
                if not accepts:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        f"computed field '{table_name}.{field}' produces"
                        f" {_STATIC_KIND_STR[kind]}, which the field type does not"
                        " accept",
                    )
        # Rule 6: authorize runs pre-stamp on the insert paths, so a predicate
        # over a computed field would read client input.
        if table.authorize is not None:
            authorize_refs: list[str] = []
            walk_filter_expr_fields(table.authorize, authorize_refs.append)
            for ref in authorize_refs:
                if ref in table.computed:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        f"computed field '{table_name}.{ref}' must not be referenced"
                        " by the table's authorize predicate (authorize predicates"
                        " may not reference computed fields)",
                    )
