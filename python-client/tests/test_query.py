"""Tests for ``par_rt_db.query``: wire ``Query`` model, ``TableQuery`` builder,
and ``parse_result`` deserialization.

Mirrors ``server/src/query.rs`` (the ``Query`` struct + ``QueryResult`` shapes)
and the builder ergonomics of ``ts-client/src/query.ts`` / ``rust-client/src/query.rs``.

Two shape corrections from the upstream brief (the plan predated a wire fix):

* ``FilterExpr`` discriminator is ``op`` (lowercase variant), NOT ``type``.
* ``VectorSearchQuery.filter`` is an eq-map ``dict[str, Any]`` over the index's
  declared ``filterFields``, NOT a nested ``FilterExpr`` (server:
  ``filter: BTreeMap<String, Value>``).
"""

import pytest
from pydantic import BaseModel, TypeAdapter

from par_rt_db.query import Paginated, TableQuery, parse_result
from par_rt_db.wire import FilterExpr

_filter_adapter = TypeAdapter(FilterExpr)


class Box(BaseModel):
    id: str
    status: str


def test_query_index_eq_range_order_take_collect():
    q = (
        TableQuery("boxes")
        .with_index("by_status")
        .eq("active")
        .gte(10)
        .lt(100)
        .order("asc")
        .take(50)
    )
    wire = q.build().model_dump(by_alias=True, mode="json")
    assert wire == {
        "table": "boxes",
        "index": "by_status",
        "eq": ["active"],
        "gte": 10,
        "lt": 100,
        "order": "asc",
        "take": 50,
    }


def test_query_get():
    assert TableQuery("boxes").get("0123").build().model_dump(by_alias=True, mode="json") == {
        "table": "boxes",
        "get": "0123",
    }


def test_query_count_and_first_and_unique_terminals():
    assert (
        TableQuery("t")
        .with_index("i")
        .eq("a")
        .build_for_count()
        .model_dump(by_alias=True, mode="json")["count"]
        is True
    )
    assert (
        TableQuery("t")
        .with_index("i")
        .eq("a")
        .build_for_first()
        .model_dump(by_alias=True, mode="json")["first"]
        is True
    )
    assert (
        TableQuery("t")
        .with_index("i")
        .eq("a")
        .build_for_unique()
        .model_dump(by_alias=True, mode="json")["unique"]
        is True
    )


def test_query_distinct_terminal():
    # `distinct` consumes one eq prefix value and distincts on the next index
    # field. Wire shape: omitted unless true (mirrors count/first/unique).
    wire = (
        TableQuery("t")
        .with_index("i")
        .eq("a")
        .build_for_distinct()
        .model_dump(by_alias=True, mode="json")
    )
    assert wire["distinct"] is True
    # A bare query with no terminal must NOT emit `distinct` (omit-when-false).
    assert "distinct" not in TableQuery("t").build().model_dump(by_alias=True, mode="json")


def test_query_get_rejects_distinct():
    # `get` is mutually exclusive with `distinct`, same shape as count/first/unique.
    with pytest.raises(ValueError):
        TableQuery("t").get("x").distinct().build()


def test_query_paginate():
    q = TableQuery("t").with_index("i").eq("a").order("desc").paginate(num_items=20)
    wire = q.build().model_dump(by_alias=True, mode="json")
    assert wire["paginate"] == {"numItems": 20}
    q2 = TableQuery("t").with_index("i").eq("a").paginate(cursor="Abc", num_items=5)
    assert q2.build().model_dump(by_alias=True, mode="json")["paginate"] == {
        "cursor": "Abc",
        "numItems": 5,
    }


def test_query_filter_uses_op_discriminator():
    # FilterExpr is tagged by `op` (lowercase), NOT `type`.
    f = _filter_adapter.validate_python({"op": "eq", "field": "status", "value": "active"})
    q = TableQuery("t").with_index("i").eq("a").filter(f).take(10)
    assert q.build().model_dump(by_alias=True, mode="json")["filter"] == {
        "op": "eq",
        "field": "status",
        "value": "active",
    }


def test_query_search():
    s = TableQuery("t").search("idx", "hello").take(5)
    assert s.build().model_dump(by_alias=True, mode="json")["search"] == {
        "index": "idx",
        "query": "hello",
    }


def test_query_vector_search_without_filter():
    v = TableQuery("t").vector_search("vidx", [1.0, 2.0], limit=3)
    assert v.build().model_dump(by_alias=True, mode="json")["vectorSearch"] == {
        "index": "vidx",
        "vector": [1.0, 2.0],
        "limit": 3,
    }


def test_query_vector_search_filter_is_eq_map():
    # vectorSearch.filter is an eq-map dict (NOT a FilterExpr); omitted when empty.
    v = TableQuery("t").vector_search("vidx", [1.0, 2.0], limit=3, filter_={"owner_id": "p1"})
    out = v.build().model_dump(by_alias=True, mode="json")["vectorSearch"]
    assert out["filter"] == {"owner_id": "p1"}
    # Empty filter dict is dropped (server's BTreeMap::is_empty skip rule).
    v_empty = TableQuery("t").vector_search("vidx", [1.0], limit=1, filter_={})
    assert "filter" not in v_empty.build().model_dump(by_alias=True, mode="json")["vectorSearch"]


def test_query_drops_none_fields():
    # The server's query is all-optional; absent fields must be omitted, not null.
    wire = TableQuery("t").build().model_dump(by_alias=True, mode="json")
    assert wire == {"table": "t"}
    assert "take" not in wire and "index" not in wire and "filter" not in wire


def test_query_terminals_mutually_exclusive_with_get():
    with pytest.raises(ValueError):
        TableQuery("t").get("x").take(5).build()


def test_parse_result_doc_docs_count_paginated():
    doc = parse_result(Box, "get", {"id": "1", "status": "a"})
    assert isinstance(doc, Box) and doc.id == "1"
    assert parse_result(Box, "get", None) is None
    docs = parse_result(Box, "collect", [{"id": "1", "status": "a"}])
    assert docs == [Box(id="1", status="a")]
    assert parse_result(Box, "count", 7) == 7
    first = parse_result(Box, "first", {"id": "9", "status": "a"})
    assert isinstance(first, Box) and first.id == "9"
    page = parse_result(Box, "paginate", {"docs": [{"id": "1", "status": "a"}], "nextCursor": "C"})
    assert isinstance(page, Paginated) and len(page.docs) == 1 and page.next_cursor == "C"
    page_end = parse_result(Box, "paginate", {"docs": []})
    assert page_end.next_cursor is None


def test_parse_result_distinct_returns_scalar_list():
    # Distinct result is a JSON array of scalar values from the server
    # (QueryResult::Distinct). Use `object` for a heterogeneous set (the
    # TypeAdapter path returns the raw value), or `str`/`float` for a
    # homogeneous index field.
    assert parse_result(object, "distinct", ["alice", "bob", "carol"]) == [
        "alice",
        "bob",
        "carol",
    ]
    assert parse_result(float, "distinct", [1.0, 2.0, 3.0]) == [1.0, 2.0, 3.0]
    assert parse_result(object, "distinct", []) == []


def test_parse_result_dict_model_returns_raw_dicts():
    out = parse_result(dict, "collect", [{"id": "1"}])
    assert out == [{"id": "1"}]
