from copy import deepcopy

import pytest
from pydantic import ValidationError

from par_rt_db import FieldExpectation, InMemoryRtDbClient, Mutation, SchemaDef, Transaction
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.query import Query


def fixture() -> tuple[InMemoryRtDbClient, str]:
    client = InMemoryRtDbClient()
    client.push_schema(
        SchemaDef.model_validate(
            {
                "tables": {
                    "rooms": {
                        "fields": {"generation": {"type": "number"}, "metadata": {"type": "any"}},
                        "indexes": [],
                        "softDelete": True,
                    },
                    "chunks": {"fields": {"name": {"type": "string"}}, "indexes": []},
                }
            }
        )
    )
    result = client.mutate(
        Mutation.builder()
        .insert(
            "rooms",
            {
                "generation": 1,
                "metadata": {
                    "base": 1,
                    "tail": 1,
                    "scope": {"backend": "wasm", "version": 1},
                    "nil": None,
                },
            },
        )
        .build()
    )
    assert result[0] is not None
    return client, str(result[0].model_dump()["id"])


def test_field_guard_wire_preserves_null_and_rejects_unknown_fields() -> None:
    txn = (
        Mutation.builder()
        .expect_fields("rooms", "row", [FieldExpectation(path=["metadata", "nil"], value=None)])
        .build()
    )
    assert txn.model_dump(by_alias=True, exclude_none=True)["steps"][0]["expected"] == [
        {"path": ["metadata", "nil"], "value": None}
    ]
    for expected in [{"path": ["metadata"]}, {"path": ["metadata"], "value": None, "extra": True}]:
        with pytest.raises(ValidationError):
            Transaction.model_validate(
                {
                    "steps": [
                        {
                            "op": "expectFields",
                            "table": "rooms",
                            "id": "row",
                            "expected": [expected],
                        }
                    ]
                }
            )


def test_field_guard_preserves_state_and_allows_tail_growth() -> None:
    client, sid = fixture()
    before = deepcopy(client.run_query(Query(table="rooms", get=sid)))
    guard = (
        Mutation.builder()
        .expect_fields(
            "rooms",
            sid,
            [
                FieldExpectation(path=["generation"], value=1.0),
                FieldExpectation(path=["metadata", "base"], value=1),
            ],
        )
        .build()
    )
    assert client.mutate(guard) == [None]
    assert client.run_query(Query(table="rooms", get=sid)) == before
    client.mutate(
        Mutation.builder().patch("rooms", sid, {"metadata": {"base": 1, "tail": 2}}).build()
    )
    assert client.mutate(guard) == [None]
    client.mutate(Mutation.builder().patch("rooms", sid, {"generation": 2}).build())
    with pytest.raises(RtDbError) as error:
        client.mutate(guard)
    assert error.value.code == ErrorCode.PRECONDITION_FAILED


def test_field_guard_rolls_back_and_compares_exact_objects_and_null() -> None:
    client, sid = fixture()
    for path, value in [
        (["metadata", "scope"], {"backend": "wasm"}),
        (["metadata", "missing"], None),
        (["generation"], True),
    ]:
        with pytest.raises(RtDbError) as error:
            client.mutate(
                Mutation.builder()
                .insert("chunks", {"name": "before"})
                .expect_fields("rooms", sid, [FieldExpectation(path=path, value=value)])
                .build()
            )
        assert error.value.code == ErrorCode.PRECONDITION_FAILED
        assert client.run_query(Query(table="chunks", take=5)) == []
    assert client.mutate(
        Mutation.builder()
        .expect_fields("rooms", sid, [FieldExpectation(path=["metadata", "nil"], value=None)])
        .build()
    ) == [None]


def test_field_guard_bounds_visibility_order_and_deadlines() -> None:
    client, sid = fixture()
    for expected in [
        [],
        [FieldExpectation(path=[], value=None)],
        [FieldExpectation(path=["metadata"], value=None)] * 2,
    ]:
        with pytest.raises(RtDbError) as error:
            client.mutate(Mutation.builder().expect_fields("rooms", sid, expected).build())
        assert error.value.code == ErrorCode.BAD_REQUEST
    with pytest.raises(RtDbError) as error:
        client.mutate(
            Mutation.builder()
            .expect_fields("rooms", sid, [FieldExpectation(path=["unknown"], value=None)])
            .build()
        )
    assert error.value.code == ErrorCode.SCHEMA_VIOLATION
    client.mutate(
        Mutation.builder()
        .patch("rooms", sid, {"generation": 2})
        .expect_fields("rooms", sid, [FieldExpectation(path=["generation"], value=2)])
        .patch("rooms", sid, {"generation": 3})
        .build()
    )
    with pytest.raises(RtDbError) as error:
        client.mutate(
            Mutation.builder()
            .expect_fields("rooms", sid, [FieldExpectation(path=["generation"], value=3)])
            .expect_before(1)
            .insert("chunks", {"name": "expired"})
            .build()
        )
    assert error.value.code == ErrorCode.PRECONDITION_FAILED
    assert client.run_query(Query(table="chunks", take=5)) == []
    client.mutate(Mutation.builder().delete("rooms", sid).build())
    with pytest.raises(RtDbError) as error:
        client.mutate(Mutation.builder().expect_fields("rooms", sid, []).build())
    assert error.value.code == ErrorCode.NOT_FOUND
