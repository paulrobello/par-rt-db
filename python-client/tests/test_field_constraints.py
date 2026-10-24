"""Declarative per-field write constraints (the server's
``TableDef::constraints``): the ``.constraints()`` builder wire shape plus
in-memory push-time and write-path enforcement.

Mirrors ``ts-client/tests/field-constraints.test.ts``; cross-engine agreement
is pinned by the ``constraint-*`` / ``error-constraint-*`` semantics-corpus
cases (the runner in ``test_semantics_corpus.py``)."""

from __future__ import annotations

from typing import Any

import pytest

from par_rt_db import Mutation
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.query import Query
from par_rt_db.schema import SchemaBuilder, t

_TABLE = "tasks"


def _tasks_schema() -> Any:
    """``tasks`` with constraints across number, int64, and string fields."""
    return (
        SchemaBuilder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("name", t.string())
                .field("score", t.number())
                .field("counter", t.int64())
                .field("note", t.optional(t.string()))
                .index("by_name", ["name"])
                .constraints(
                    {
                        "score": {"min": 1, "max": 10},
                        "counter": {"min": 1, "max": 100},
                        "name": {"minLength": 2, "maxLength": 5, "pattern": "^[a-z]+$"},
                    }
                )
            ),
        )
        .build()
    )


def _client() -> InMemoryRtDbClient:
    return InMemoryRtDbClient(
        InMemoryRtDbClientOptions(now=lambda: 1_700_000_000_000, random=lambda: 0.0)
    )


def _run(c: InMemoryRtDbClient, steps: list[dict[str, Any]]) -> Any:
    return c.mutate(Mutation.model_validate({"steps": steps}))


class TestBuilderWireShape:
    def test_round_trips_constraints_on_the_wire_omitted_when_absent(self) -> None:
        schema = _tasks_schema()
        table = schema.tables[_TABLE]
        assert table.model_dump(by_alias=True)["constraints"] == {
            "score": {"min": 1, "max": 10},
            "counter": {"min": 1, "max": 100},
            "name": {"minLength": 2, "maxLength": 5, "pattern": "^[a-z]+$"},
        }

        plain = SchemaBuilder().table("plain", lambda tb: tb.field("name", t.string())).build()
        assert "constraints" not in plain.tables["plain"].model_dump(by_alias=True)

    def test_empty_constraints_map_is_omitted(self) -> None:
        schema = (
            SchemaBuilder()
            .table("plain", lambda tb: tb.field("name", t.string()).constraints({}))
            .build()
        )
        assert "constraints" not in schema.tables["plain"].model_dump(by_alias=True)


class TestPushTimeValidation:
    def test_rejects_a_misapplied_constraint_naming_the_field(self) -> None:
        schema = (
            SchemaBuilder()
            .table(
                "tasks",
                lambda tb: (
                    tb.field("name", t.string())
                    .index("by_name", ["name"])
                    .constraints({"name": {"min": 1}})
                ),
            )
            .build()
        )
        c = _client()
        with pytest.raises(RtDbError) as ei:
            c.push_schema(schema)
        assert ei.value.code == ErrorCode.SCHEMA_VIOLATION
        assert "'name'" in ei.value.message
        assert "number/int64" in ei.value.message

    def test_rejects_an_invalid_regex_naming_the_field(self) -> None:
        schema = (
            SchemaBuilder()
            .table(
                "tasks",
                lambda tb: tb.field("name", t.string()).constraints({"name": {"pattern": "([a-z"}}),
            )
            .build()
        )
        c = _client()
        with pytest.raises(RtDbError) as ei:
            c.push_schema(schema)
        assert ei.value.code == ErrorCode.SCHEMA_VIOLATION
        assert "invalid pattern" in ei.value.message

    def test_int64_bounds_must_be_integers(self) -> None:
        schema = (
            SchemaBuilder()
            .table(
                "tasks",
                lambda tb: (
                    tb.field("name", t.string())
                    .field("counter", t.int64())
                    .constraints({"counter": {"min": 1.5}})
                ),
            )
            .build()
        )
        c = _client()
        with pytest.raises(RtDbError) as ei:
            c.push_schema(schema)
        assert ei.value.code == ErrorCode.SCHEMA_VIOLATION
        assert "integer" in ei.value.message


class TestWritePathEnforcement:
    def test_accepts_inclusive_boundary_values(self) -> None:
        c = _client()
        c.push_schema(_tasks_schema())
        _run(
            c,
            [
                {
                    "op": "insert",
                    "table": _TABLE,
                    "doc": {"name": "aa", "score": 1, "counter": "1"},
                },
                {
                    "op": "insert",
                    "table": _TABLE,
                    "doc": {"name": "abcde", "score": 10, "counter": "100"},
                },
            ],
        )

    def test_fails_an_insert_below_min_and_rolls_back(self) -> None:
        c = _client()
        c.push_schema(_tasks_schema())
        with pytest.raises(RtDbError) as ei:
            _run(
                c,
                [
                    {
                        "op": "insert",
                        "table": _TABLE,
                        "doc": {"name": "good", "score": 5, "counter": "1"},
                    },
                    {
                        "op": "insert",
                        "table": _TABLE,
                        "doc": {"name": "bad", "score": 0.5, "counter": "1"},
                    },
                ],
            )
        assert ei.value.code == ErrorCode.SCHEMA_VIOLATION
        assert c.run_query(Query.model_validate({"table": _TABLE})) == []

    def test_enforces_length_and_pattern_on_strings(self) -> None:
        c = _client()
        c.push_schema(_tasks_schema())
        with pytest.raises(RtDbError, match=r"'minLength' constraint"):
            _run(
                c,
                [
                    {
                        "op": "insert",
                        "table": _TABLE,
                        "doc": {"name": "a", "score": 5, "counter": "1"},
                    }
                ],
            )
        with pytest.raises(RtDbError, match=r"'maxLength' constraint"):
            _run(
                c,
                [
                    {
                        "op": "insert",
                        "table": _TABLE,
                        "doc": {"name": "abcdef", "score": 5, "counter": "1"},
                    }
                ],
            )
        with pytest.raises(RtDbError, match=r"'pattern' constraint"):
            _run(
                c,
                [
                    {
                        "op": "insert",
                        "table": _TABLE,
                        "doc": {"name": "ABC", "score": 5, "counter": "1"},
                    }
                ],
            )

    def test_enforces_through_patch_replace_upsert_and_patch_by_query(self) -> None:
        c = _client()
        c.push_schema(_tasks_schema())
        [res] = _run(
            c,
            [{"op": "insert", "table": _TABLE, "doc": {"name": "ok", "score": 5, "counter": "1"}}],
        )
        assert res is not None
        doc_id = str(res.model_dump()["id"])

        with pytest.raises(RtDbError, match=r"'max' constraint"):
            _run(c, [{"op": "patch", "table": _TABLE, "id": doc_id, "fields": {"score": 50}}])
        with pytest.raises(RtDbError):
            _run(
                c,
                [
                    {
                        "op": "replace",
                        "table": _TABLE,
                        "id": doc_id,
                        "doc": {"name": "ab", "score": 99, "counter": "2"},
                    }
                ],
            )
        with pytest.raises(RtDbError, match=r"'max' constraint"):
            _run(
                c,
                [
                    {
                        "op": "upsert",
                        "table": _TABLE,
                        "index": "by_name",
                        "eq": ["ok"],
                        "insert": {"name": "ok", "score": 5, "counter": "1"},
                        "patch": {"counter": "101"},
                    }
                ],
            )
        with pytest.raises(RtDbError, match=r"'max' constraint"):
            _run(
                c,
                [
                    {
                        "op": "patchByQuery",
                        "table": _TABLE,
                        "filter": {"field": "name", "op": "eq", "value": "ok"},
                        "patch": {"score": 99},
                    }
                ],
            )
        # Nothing survived: the stored doc is untouched.
        assert c.run_query(Query.model_validate({"table": _TABLE})) == [
            {
                "_id": doc_id,
                "name": "ok",
                "score": 5,
                "counter": "1",
                "_version": 1,
                "_creationTime": 1_700_000_000_000,
            }
        ]

    def test_never_violates_on_null_or_absent_values(self) -> None:
        c = _client()
        c.push_schema(_tasks_schema())
        _run(
            c,
            [
                {
                    "op": "insert",
                    "table": _TABLE,
                    "doc": {"name": "ok", "score": 5, "counter": "1", "note": None},
                }
            ],
        )


def test_schema_export_unchanged() -> None:
    """A constraints-carrying SchemaDef survives a to_schema_json round-trip."""
    c = _client()
    schema = _tasks_schema()
    c.push_schema(schema)
    exported = c.to_schema_json()
    assert exported is not None
    assert exported.tables[_TABLE].constraints == schema.tables[_TABLE].constraints
