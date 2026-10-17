"""Bounded, read-only document-field equality assertions."""

from __future__ import annotations

import math
from typing import Any

from ..errors import ErrorCode, RtDbError
from ..mutation import FieldExpectation
from ..schema import TableDef
from .store import _json_structurally_equal


def _field_number(value: int | float) -> tuple[int | None, float]:
    # serde_json preserves i64/u64 integers; larger JSON literals use f64.
    if isinstance(value, int) and -(2**63) <= value <= 2**64 - 1:
        return value, float(value)
    try:
        number = float(value)
    except OverflowError:
        return None, math.inf if value > 0 else -math.inf
    if math.isfinite(number) and number.is_integer() and abs(number) < 2**127:
        return int(number), number
    return None, number


def _field_json_equal(left: Any, right: Any) -> bool:
    if (
        isinstance(left, (int, float))
        and isinstance(right, (int, float))
        and not isinstance(left, bool)
        and not isinstance(right, bool)
    ):
        left_integer, left_float = _field_number(left)
        right_integer, right_float = _field_number(right)
        if left_integer is not None and right_integer is not None:
            return left_integer == right_integer
        return left_float == right_float
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _field_json_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _field_json_equal(first, second) for first, second in zip(left, right, strict=True)
        )
    return _json_structurally_equal(left, right)


def _declared_path(fields: dict[str, Any], path: list[str]) -> bool:
    return path[0] in fields and _declared_child(fields[path[0]], path[1:])


def _declared_child(field: dict[str, Any], path: list[str]) -> bool:
    if not path:
        return True
    match field["type"]:
        case "any":
            return True
        case "optional":
            return _declared_child(field["inner"], path)
        case "object":
            return _declared_path(field["fields"], path)
        case "record":
            return _declared_child(field["value"], path[1:])
        case "union":
            return any(_declared_child(variant, path) for variant in field["variants"])
        case _:
            return False


def check_field_expectations(
    table: TableDef, doc: dict[str, Any], expected: list[FieldExpectation]
) -> None:
    """Validate after locating the live row, then compare without mutating it."""
    if not 1 <= len(expected) <= 64:
        raise RtDbError(ErrorCode.BAD_REQUEST, "expected must contain 1-64 field assertions")
    fields = {name: field.model_dump(by_alias=True) for name, field in table.fields.items()}
    paths: set[tuple[str, ...]] = set()
    nodes = 0
    byte_count = 0

    def utf8_length(value: str) -> int:
        try:
            return len(value.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise RtDbError(
                ErrorCode.BAD_REQUEST, "expected strings must be valid UTF-8"
            ) from error

    def invalid_budget() -> None:
        raise RtDbError(
            ErrorCode.BAD_REQUEST,
            "expected field values exceed the depth, node or UTF-8 byte budget",
        )

    def budget(value: Any, depth: int) -> None:
        nonlocal nodes, byte_count
        nodes += 1
        if depth > 32 or nodes > 8192 or byte_count > 65_536:
            invalid_budget()
        if isinstance(value, str):
            byte_count += utf8_length(value)
        elif isinstance(value, list):
            for child in value:
                budget(child, depth + 1)
        elif isinstance(value, dict):
            for key, child in value.items():
                if not isinstance(key, str):
                    invalid_budget()
                byte_count += utf8_length(key)
                budget(child, depth + 1)
        elif isinstance(value, (int, float)):
            if not math.isfinite(_field_number(value)[1]):
                invalid_budget()
        elif value is not None:
            invalid_budget()
        if byte_count > 65_536:
            invalid_budget()

    for item in expected:
        if not 1 <= len(item.path) <= 16 or any(
            not key or utf8_length(key) > 256 for key in item.path
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST, "field paths require 1-16 keys of 1-256 UTF-8 bytes"
            )
        path = tuple(item.path)
        if path in paths:
            raise RtDbError(ErrorCode.BAD_REQUEST, "expected contains a duplicate field path")
        paths.add(path)
        byte_count += sum(utf8_length(key) for key in item.path)
        budget(item.value, 0)
        if not _declared_path(fields, item.path):
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, "expected field path is not declared")

    for item in expected:
        value: Any = doc
        for key in item.path:
            if not isinstance(value, dict) or key not in value:
                raise RtDbError(ErrorCode.PRECONDITION_FAILED, "expected fields did not match")
            value = value[key]
        if not _field_json_equal(value, item.value):
            raise RtDbError(ErrorCode.PRECONDITION_FAILED, "expected fields did not match")
