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


_STAMP_CORPUS = json.loads(
    (
        Path(__file__).resolve().parents[2] / "wire-corpus" / "spec" / "computed-stamping.json"
    ).read_text(encoding="utf-8")
)


def test_every_computed_stamping_op_has_a_fixture_row() -> None:
    """The corpus README's own-op coverage rule: every ValueExpr op this
    engine's ``eval_value_expr`` distinguishes must be exercised by a fixture
    row — a grammar node without a row fails."""
    seen: set[str] = set()
    for row in _STAMP_CORPUS["rows"]:
        for expr in row["computed"].values():
            seen.add(expr["op"])
    for op in (
        "field",
        "literal",
        "concat",
        "add",
        "sub",
        "mul",
        "div",
        "coalesce",
        "lower",
        "upper",
        "trim",
        "cast",
        "now",
        "case",
    ):
        assert op in seen, f"no fixture row exercises op '{op}'"


def test_every_computed_stamping_row_matches_this_engine() -> None:
    """Each row's wire schema fragment is decoded through the client's real
    pydantic ``TableDef`` (the same spelling ``pushSchema`` parses), stamped
    with the engine's ``_stamp_computed``, and asserted per key — an expected
    JSON null means the key must be ABSENT after stamping; anything else
    compares with ``==``, which treats ``42`` and ``42.0`` as equal the way
    the cross-engine contract requires."""
    from par_rt_db.in_memory.value_expr import _stamp_computed
    from par_rt_db.schema import TableDef

    adapter = TypeAdapter(TableDef)
    for i, row in enumerate(_STAMP_CORPUS["rows"]):
        table = adapter.validate_python({"fields": row["fields"], "computed": row["computed"]})
        doc = dict(row["inputDoc"])
        if row["expect"].get("error"):
            with pytest.raises(RtDbError) as ei:
                _stamp_computed(table, doc, row["now"])
            assert ei.value.code is ErrorCode.BAD_REQUEST, f"row {i}: error code"
            continue
        stamped = _stamp_computed(table, doc, row["now"])
        for name, expected in row["expect"]["stampedValues"].items():
            if expected is None:
                assert name not in stamped, f"row {i}: key '{name}' must be absent after stamping"
            else:
                assert name in stamped, f"row {i}: missing computed key '{name}'"
                assert stamped[name] == expected, f"row {i}: stamped value of '{name}'"


def _aggregate_corpus() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[2] / "wire-corpus" / "spec" / "aggregate-terminals.json"
    return json.loads(path.read_text())


def test_every_aggregate_op_has_a_fixture_row() -> None:
    """The corpus README's own-op coverage rule: every AggregateOp this
    engine distinguishes must be exercised by a fixture row."""
    seen: set[str] = set()
    for row in _aggregate_corpus()["rows"]:
        agg = row["aggregate"]
        if "op" in agg:
            seen.add(agg["op"])
        for op in (agg.get("aggregates") or {}).values():
            seen.add(op)
    for op in ("sum", "avg", "min", "max", "count"):
        assert op in seen, f"no fixture row exercises aggregate op '{op}'"


def test_every_aggregate_row_matches_this_engine() -> None:
    """Each row pushes its schema fragment, inserts the docs through the real
    write path, runs the aggregate query through ``run_query``, and asserts
    the bare wire value (``expect.error`` rows assert the failure)."""
    from pydantic import ValidationError

    from par_rt_db.in_memory import InMemoryRtDbClient
    from par_rt_db.in_memory.store import InMemoryRtDbClientOptions
    from par_rt_db.mutation import Mutation
    from par_rt_db.query import Query
    from par_rt_db.schema import SchemaDef

    def make_runner(client: Any) -> Any:
        """A shape Query validates eagerly raises at model_validate; both
        that and an execution-time rejection surface as RtDbError."""

        def run_query(q_json: dict[str, Any]) -> Any:
            try:
                q = Query.model_validate(q_json)
            except ValidationError as err:
                raise RtDbError(ErrorCode.BAD_REQUEST, str(err)) from err
            return client.run_query(q)

        return run_query

    for i, row in enumerate(_aggregate_corpus()["rows"]):
        ms = 1_700_000_000_000

        def _now() -> int:
            nonlocal ms
            ms += 1
            return ms

        client = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=_now, random=lambda: 0.0))
        client.push_schema(
            SchemaDef.model_validate(
                {
                    "tables": {
                        "items": {
                            "fields": row["fields"],
                            "indexes": [row["index"]],
                        }
                    }
                }
            )
        )
        client.mutate(Mutation.builder().insert_many("items", row["docs"]).build())
        q_json = {
            "table": "items",
            "index": row["index"]["name"],
            "eq": row["eq"],
            "aggregate": row["aggregate"],
        }
        run_query = make_runner(client)
        expect = row["expect"]
        if isinstance(expect, dict) and "error" in expect:
            with pytest.raises(RtDbError):
                run_query(q_json)
            continue
        assert run_query(q_json) == expect, f"row {i} ({row['id']}): aggregate result"


# ---------------------------------------------------------------------------
# ENH-052 family 4 — spec-table corpus: the btree keyset-pagination decision
# table (wire-corpus/spec/pagination.json). Each row pushes its schema
# fragment, inserts the docs through the real write path, encodes the RAW
# cursor values through this engine's codec, runs the paginate query through
# ``run_query``, and asserts the page envelope — ``nextCursorPrefix`` is the
# unbound-index prefix of the decoded minted cursor (``expect.error`` rows
# assert the failure with its code).
# ---------------------------------------------------------------------------

_PAGINATION_CORPUS_PATH = (
    Path(__file__).resolve().parents[2] / "wire-corpus" / "spec" / "pagination.json"
)
_PAGINATION_CORPUS = json.loads(_PAGINATION_CORPUS_PATH.read_text(encoding="utf-8"))


def test_every_pagination_order_and_kind_has_a_fixture_row() -> None:
    """The corpus README's own-coverage rule: every Order spelling and keyset
    value kind this engine distinguishes must be exercised by a fixture row."""
    orders: set[str] = set()
    kinds: set[str] = set()
    for row in _PAGINATION_CORPUS["rows"]:
        orders.add(row["order"])
        kinds.add(row["fields"]["v"]["type"])
    for order in ("asc", "desc"):
        assert order in orders, f"no fixture row exercises order '{order}'"
    for kind in ("number", "int64", "boolean", "optional"):
        assert kind in kinds, f"no fixture row exercises value kind '{kind}'"


def test_every_pagination_row_matches_this_engine() -> None:
    from par_rt_db.cursor import decode_cursor, encode_cursor
    from par_rt_db.errors import ErrorCode
    from par_rt_db.in_memory import InMemoryRtDbClient
    from par_rt_db.in_memory.store import InMemoryRtDbClientOptions
    from par_rt_db.mutation import Mutation
    from par_rt_db.query import Query
    from par_rt_db.schema import SchemaDef

    SYSTEM_KEYS = ("_id", "_creationTime", "_version")

    for i, row in enumerate(_PAGINATION_CORPUS["rows"]):
        ms = 1_700_000_000_000

        def _now() -> int:
            nonlocal ms
            ms += 1
            return ms

        client = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=_now, random=lambda: 0.0))
        client.push_schema(
            SchemaDef.model_validate(
                {
                    "tables": {
                        "items": {
                            "fields": row["fields"],
                            "indexes": [row["index"]],
                        }
                    }
                }
            )
        )
        try:
            client.mutate(Mutation.builder().insert_many("items", row["docs"]).build())
        except RtDbError as err:
            raise AssertionError(f"row {i} ({row['id']}): insert failed: {err}") from err
        paginate: dict[str, Any] = {"numItems": row["paginate"]["numItems"]}
        if "cursor" in row["paginate"]:
            paginate["cursor"] = encode_cursor(row["paginate"]["cursor"])
        q_json = {
            "table": "items",
            "index": row["index"]["name"],
            "eq": row["eq"],
            "order": row["order"],
            "paginate": paginate,
        }
        try:
            q = Query.model_validate(q_json)
        except Exception as err:
            raise AssertionError(f"row {i} ({row['id']}): query does not parse: {err}") from err
        want_code = (row["expect"].get("error") or {}).get("code")
        if want_code is not None:
            try:
                result = client.run_query(q)
            except RtDbError as err:
                assert err.code == ErrorCode[want_code], (
                    f"row {i} ({row['id']}): rejection code {err.code}"
                )
                continue
            raise AssertionError(f"row {i} ({row['id']}): expected the query to fail, got {result}")
        result = client.run_query(q)
        got_docs = [
            {k: v for k, v in doc.items() if k not in SYSTEM_KEYS} for doc in result["docs"]
        ]
        assert got_docs == row["expect"]["docs"], f"row {i} ({row['id']}): page docs"
        prefix = row["expect"].get("nextCursorPrefix")
        if prefix is None:
            assert "nextCursor" not in result, f"row {i} ({row['id']}): expected no next cursor"
            continue
        assert "nextCursor" in result, f"row {i} ({row['id']}): expected a next cursor"
        decoded = decode_cursor(result["nextCursor"])
        assert len(decoded) == len(prefix) + 2, f"row {i} ({row['id']}): minted cursor arity"
        assert decoded[:-2] == prefix, f"row {i} ({row['id']}): minted cursor prefix"
        assert isinstance(decoded[-2], (int, float)), f"row {i} ({row['id']}): created_at kind"
        assert isinstance(decoded[-1], str), f"row {i} ({row['id']}): id kind"


# ---------------------------------------------------------------------------
# ENH-052 family 5 — spec-table corpus: the push-schema validation decision
# table (wire-corpus/spec/validation.json). Each row pushes its wire schema
# fragment through the real ``push_schema`` path and asserts the same
# accept/reject outcome with the same code as the server's own
# ``SchemaDef::validate``.
# ---------------------------------------------------------------------------

_VALIDATION_CORPUS_PATH = (
    Path(__file__).resolve().parents[2] / "wire-corpus" / "spec" / "validation.json"
)
_VALIDATION_CORPUS = json.loads(_VALIDATION_CORPUS_PATH.read_text(encoding="utf-8"))

_OWN_RULE_FAMILIES = (
    "index",
    "ttl",
    "updated-at",
    "auto-increment",
    "on-delete",
    "constraints",
    "computed",
    "accept",
)


def test_every_validation_rule_family_has_a_fixture_row() -> None:
    """The corpus README's own-coverage rule: every rule family this engine
    mirrors must be exercised by a fixture row."""
    ids = [row["id"] for row in _VALIDATION_CORPUS["rows"]]
    for family in _OWN_RULE_FAMILIES:
        assert any(i.startswith(family) for i in ids), (
            f"no fixture row exercises rule family '{family}'"
        )


def test_every_validation_row_matches_this_engine() -> None:
    from par_rt_db.in_memory import InMemoryRtDbClient
    from par_rt_db.in_memory.store import InMemoryRtDbClientOptions
    from par_rt_db.schema import SchemaDef

    for i, row in enumerate(_VALIDATION_CORPUS["rows"]):
        ms = 1_700_000_000_000

        def _now() -> int:
            nonlocal ms
            ms += 1
            return ms

        client = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=_now, random=lambda: 0.0))
        want_code = (row["expect"].get("error") or {}).get("code")
        got_code: str | None
        try:
            client.push_schema(SchemaDef.model_validate(row["schema"]))
            got_code = None
        except RtDbError as err:
            got_code = err.code.value
        if want_code is not None:
            assert got_code == want_code, f"row {i} ({row['id']}): rejection code {got_code}"
            continue
        assert got_code is None, (
            f"row {i} ({row['id']}): expected the push to accept, got {got_code}"
        )
