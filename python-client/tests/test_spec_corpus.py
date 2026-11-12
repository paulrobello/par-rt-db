"""ENH-052 family 1 — spec-table corpus runner: the FieldType -> Postgres
column-type mapping (``wire-corpus/spec/index-column-types.json``).

Asserts the in-memory engine's ``_index_column_type`` against every fixture
row — decoded through the client's real pydantic ``FieldType`` union, the same
spelling ``pushSchema`` parses — and that this implementation's FieldType
variant set is covered by the fixture. ``columnType`` carries the server's raw
Postgres string; the engine's ``_PgType`` four-value sentinels map back to it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import TypeAdapter

from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory.store import _index_column_type
from par_rt_db.schema import FieldType

_CORPUS = json.loads(
    (
        Path(__file__).resolve().parents[2] / "wire-corpus" / "spec" / "index-column-types.json"
    ).read_text(encoding="utf-8")
)

_ADAPTER = TypeAdapter(FieldType)

#: The engine's four-value storage sentinels mapped back to the server's raw
#: column strings.
_RAW_PG: dict[str, str] = {
    "text": "text",
    "number": "double precision",
    "boolean": "boolean",
    "int64": "bigint",
}

#: One representative per FieldType variant this engine's
#: ``_index_column_type`` distinguishes, in the wire spelling (the corpus
#: README's own-variant coverage rule: a variant without a fixture row fails).
_OWN_VARIANTS: list[dict[str, Any]] = [
    {"type": "string"},
    {"type": "number"},
    {"type": "boolean"},
    {"type": "int64"},
    {"type": "id", "table": "t"},
    {"type": "id", "table": "t", "onDelete": "cascade"},
    {"type": "literal", "value": "x"},
    {"type": "literal", "value": 42},
    {"type": "optional", "inner": {"type": "string"}},
    {
        "type": "union",
        "variants": [
            {"type": "literal", "value": "a"},
            {"type": "literal", "value": "b"},
        ],
    },
    {"type": "union", "variants": []},
    {"type": "union", "variants": [{"type": "literal", "value": "a"}, {"type": "number"}]},
    {"type": "null"},
    {"type": "array", "element": {"type": "string"}},
    {"type": "object", "fields": {"a": {"type": "string"}}},
    {"type": "bytes"},
    {"type": "any"},
    {"type": "record", "value": {"type": "string"}},
    {"type": "vector", "dimensions": 3},
]


def _canonical(value: Any) -> str:
    if isinstance(value, list):
        return "[" + ",".join(_canonical(v) for v in value) + "]"
    if isinstance(value, dict):
        return (
            "{"
            + ",".join(json.dumps(k) + ":" + _canonical(v) for k, v in sorted(value.items()))
            + "}"
        )
    return json.dumps(value)


def test_every_own_variant_has_a_fixture_row() -> None:
    rows = {_canonical(row["fieldType"]) for row in _CORPUS["rows"]}
    for ty in _OWN_VARIANTS:
        assert _canonical(ty) in rows, f"no fixture row for {_canonical(ty)}"


def test_every_fixture_row_matches_this_engine() -> None:
    for row in _CORPUS["rows"]:
        if row.get("error"):
            with pytest.raises(RtDbError) as ei:
                _index_column_type(_ADAPTER.validate_python(row["fieldType"]))
            assert ei.value.code is ErrorCode.SCHEMA_VIOLATION
            continue
        resolved = _index_column_type(_ADAPTER.validate_python(row["fieldType"]))
        assert _RAW_PG[resolved.pg] == row["columnType"]
        assert resolved.nullable == row["nullable"]
