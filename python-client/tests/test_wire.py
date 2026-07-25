"""Wire-shape tests for par_rt_db.wire leaf types.

These mirror the on-the-wire JSON shapes defined by:
  - server/src/protocol.rs  (AuthedUser, ScheduleWhen, ScheduleInfo)
  - server/src/query.rs      (FilterExpr, SearchQuery, VectorSearchQuery)
and cross-checked against ts-client/src/protocol.ts and rust-client/src/wire.rs.

Discriminator field names are load-bearing: FilterExpr is tagged by ``op`` (not
``type``), and ``VectorSearchQuery.filter`` is an eq-map (``Record<string, unknown>``),
not a nested ``FilterExpr``.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from par_rt_db.wire import (
    AuthedUser,
    FilterExpr,
    ScheduleInfo,
    ScheduleWhen,
    SearchQuery,
    VectorSearchQuery,
)


def test_authed_user_minimal_includes_null_email_name_omits_github() -> None:
    u = AuthedUser.model_validate({"kind": "user"})
    dumped = u.model_dump(by_alias=True, mode="json")
    assert dumped["kind"] == "user"
    assert "email" in dumped and dumped["email"] is None  # null on wire
    assert "name" in dumped and dumped["name"] is None  # null on wire
    assert "githubLogin" not in dumped  # omitted when absent
    assert "githubId" not in dumped


def test_authed_user_full() -> None:
    u = AuthedUser.model_validate(
        {
            "kind": "machine",
            "email": "a@b.com",
            "name": "A",
            "githubLogin": "oct",
            "githubId": 7,
        }
    )
    assert u.model_dump(by_alias=True, mode="json") == {
        "kind": "machine",
        "email": "a@b.com",
        "name": "A",
        "githubLogin": "oct",
        "githubId": 7,
    }


def test_authed_user_rejects_unknown() -> None:
    with pytest.raises(ValidationError):
        AuthedUser.model_validate({"kind": "user", "bogus": 1})


def test_schedule_when_variants() -> None:
    adapter = TypeAdapter(ScheduleWhen)
    assert adapter.validate_python({"type": "afterMs", "ms": 5}).model_dump(
        by_alias=True, mode="json"
    ) == {"type": "afterMs", "ms": 5}
    assert adapter.validate_python({"type": "runAt", "ms": 9}).model_dump(
        by_alias=True, mode="json"
    ) == {"type": "runAt", "ms": 9}
    assert adapter.validate_python({"type": "cron", "expr": "*/5 * * * *"}).model_dump(
        by_alias=True, mode="json"
    ) == {"type": "cron", "expr": "*/5 * * * *"}


def test_schedule_info_omits_optional_when_absent() -> None:
    si = ScheduleInfo.model_validate(
        {
            "id": "j1",
            "kind": "oneshot",
            "dueAt": 100,
            "status": "pending",
            "createdAt": 1,
            "firedCount": 0,
        }
    )
    d = si.model_dump(by_alias=True, mode="json")
    assert d["id"] == "j1" and d["dueAt"] == 100 and d["firedCount"] == 0
    assert "cron" not in d and "lastError" not in d


def test_filter_expr_leaves_and_combinators() -> None:
    adapter = TypeAdapter(FilterExpr)
    eq = adapter.validate_python({"op": "eq", "field": "status", "value": "active"})
    assert eq.model_dump(by_alias=True, mode="json") == {
        "op": "eq",
        "field": "status",
        "value": "active",
    }
    inv = adapter.validate_python({"op": "in", "field": "status", "values": ["a", "b"]})
    assert inv.model_dump(by_alias=True, mode="json") == {
        "op": "in",
        "field": "status",
        "values": ["a", "b"],
    }
    and_ = adapter.validate_python(
        {
            "op": "and",
            "exprs": [
                {"op": "eq", "field": "a", "value": 1},
                {"op": "or", "exprs": []},
            ],
        }
    )
    dumped = and_.model_dump(by_alias=True, mode="json")
    assert dumped["op"] == "and"
    assert dumped["exprs"][0] == {"op": "eq", "field": "a", "value": 1}
    assert dumped["exprs"][1] == {"op": "or", "exprs": []}


def test_search_query_shape() -> None:
    sq = SearchQuery.model_validate({"index": "idx", "query": "hello"})
    assert sq.model_dump(by_alias=True, mode="json") == {"index": "idx", "query": "hello"}


def test_vector_search_query_omits_filter_when_none() -> None:
    vq = VectorSearchQuery.model_validate({"index": "v", "vector": [0.1, 0.2], "limit": 8})
    out = vq.model_dump(by_alias=True, mode="json")
    assert out["index"] == "v" and out["vector"] == [0.1, 0.2] and out["limit"] == 8
    assert "filter" not in out


def test_vector_search_query_omits_filter_when_empty() -> None:
    # Server uses BTreeMap::is_empty — empty filter must also drop.
    vq = VectorSearchQuery.model_validate({"index": "v", "vector": [0.1], "limit": 1, "filter": {}})
    assert "filter" not in vq.model_dump(by_alias=True, mode="json")


def test_vector_search_query_keeps_filter_when_non_empty() -> None:
    vq = VectorSearchQuery.model_validate(
        {"index": "v", "vector": [0.1], "limit": 1, "filter": {"status": "active"}}
    )
    out = vq.model_dump(by_alias=True, mode="json")
    assert out["filter"] == {"status": "active"}
