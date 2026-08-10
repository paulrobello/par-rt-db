"""Tests for ``par_rt_db.query``: wire ``Query`` model, ``TableQuery`` builder,
and ``parse_result`` deserialization.

Mirrors ``server/src/query.rs`` (the ``Query`` struct + ``QueryResult`` shapes)
and the builder ergonomics of ``ts-client/src/query.ts`` / ``rust-client/src/query.rs``.

Two shape corrections from the upstream brief (the plan predated a wire fix):

* ``FilterExpr`` discriminator is ``op`` (lowercase variant), NOT ``type``.
* ``VectorSearchQuery.filter`` is the full ``FilterExpr`` (the same type
  ``.filter()`` and ``search`` use), omitted when ``None``.
"""

import pytest
from pydantic import BaseModel, TypeAdapter

from par_rt_db.query import (
    Paginated,
    Query,
    TableQuery,
    _dump_query,
    _terminal_of,
    parse_result,
)
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


def test_query_aggregate_terminal():
    # `aggregate` (no groupBy): `{op}` wire shape (groupBy defaults False and is
    # omitted on the wire by the python mirror; the server's `#[serde(default)]`
    # accepts either form).
    wire = (
        TableQuery("t")
        .with_index("i")
        .eq("a")
        .build_for_aggregate("sum")
        .model_dump(by_alias=True, mode="json")
    )
    assert wire["aggregate"] == {"op": "sum"}
    # A bare query with no terminal must NOT emit `aggregate`.
    assert "aggregate" not in TableQuery("t").build().model_dump(by_alias=True, mode="json")


def test_query_aggregate_terminal_group_by():
    # groupBy=True emits the camelCase flag on the wire.
    wire = (
        TableQuery("t")
        .with_index("i")
        .eq("a")
        .build_for_aggregate("sum", group_by=True)
        .model_dump(by_alias=True, mode="json")
    )
    assert wire["aggregate"] == {"op": "sum", "groupBy": True}


def test_query_aggregate_terminal_count():
    # `count` serializes its lowercase op tag like the other aggregates.
    wire = (
        TableQuery("t")
        .with_index("i")
        .eq("a")
        .build_for_aggregate("count")
        .model_dump(by_alias=True, mode="json")
    )
    assert wire["aggregate"] == {"op": "count"}
    # groupBy + count composes on the wire too.
    wire_g = (
        TableQuery("t")
        .with_index("i")
        .eq("a")
        .build_for_aggregate("count", group_by=True)
        .model_dump(by_alias=True, mode="json")
    )
    assert wire_g["aggregate"] == {"op": "count", "groupBy": True}


def test_query_get_rejects_aggregate():
    # `get` is mutually exclusive with `aggregate`, same shape as count/distinct.
    with pytest.raises(ValueError):
        TableQuery("t").get("x").aggregate("sum").build()


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


def test_query_search_omits_filter_when_absent():
    # No filter= ⇒ the key is omitted entirely (existing requests stay byte-identical).
    out = (
        TableQuery("t")
        .search("idx", "hello")
        .build()
        .model_dump(by_alias=True, mode="json")["search"]
    )
    assert "filter" not in out


def test_query_search_serializes_full_filter_expr():
    # search's filter is the FULL FilterExpr (not vector search's eq-map).
    flt = _filter_adapter.validate_python(
        {
            "op": "and",
            "exprs": [
                {"op": "eq", "field": "channel", "value": "#general"},
                {"op": "gt", "field": "createdAt", "value": 1780000000000},
            ],
        }
    )
    out = (
        TableQuery("t")
        .search("idx", "hi", filter_=flt)
        .build()
        .model_dump(by_alias=True, mode="json")["search"]
    )
    assert out == {
        "index": "idx",
        "query": "hi",
        "filter": {
            "op": "and",
            "exprs": [
                {"op": "eq", "field": "channel", "value": "#general"},
                {"op": "gt", "field": "createdAt", "value": 1780000000000},
            ],
        },
    }


def test_query_vector_search_without_filter():
    v = TableQuery("t").vector_search("vidx", [1.0, 2.0], limit=3)
    assert v.build().model_dump(by_alias=True, mode="json")["vectorSearch"] == {
        "index": "vidx",
        "vector": [1.0, 2.0],
        "limit": 3,
    }


def test_query_vector_search_serializes_full_filter_expr():
    # vectorSearch's filter is the FULL FilterExpr (the same type search uses).
    flt = _filter_adapter.validate_python(
        {
            "op": "and",
            "exprs": [
                {"op": "eq", "field": "channel", "value": "#general"},
                {"op": "gt", "field": "createdAt", "value": 1780000000000},
            ],
        }
    )
    out = (
        TableQuery("t")
        .vector_search("vidx", [1.0, 2.0], limit=3, filter_=flt)
        .build()
        .model_dump(by_alias=True, mode="json")["vectorSearch"]
    )
    assert out == {
        "index": "vidx",
        "vector": [1.0, 2.0],
        "limit": 3,
        "filter": {
            "op": "and",
            "exprs": [
                {"op": "eq", "field": "channel", "value": "#general"},
                {"op": "gt", "field": "createdAt", "value": 1780000000000},
            ],
        },
    }


def test_query_hybrid_search_required_only():
    h = TableQuery("t").hybrid_search("hello", [1.0, 0.0, 0.0], limit=5)
    assert h.build().model_dump(by_alias=True, mode="json")["hybridSearch"] == {
        "query": "hello",
        "vector": [1.0, 0.0, 0.0],
        "limit": 5,
    }


def test_query_hybrid_search_optional_fields_round_trip():
    h = TableQuery("t").hybrid_search(
        "hello",
        [1.0, 0.0, 0.0],
        limit=5,
        search_index="search_body",
        vector_index="by_embedding",
        k=42,
    )
    out = h.build().model_dump(by_alias=True, mode="json")["hybridSearch"]
    assert out == {
        "query": "hello",
        "vector": [1.0, 0.0, 0.0],
        "limit": 5,
        "searchIndex": "search_body",
        "vectorIndex": "by_embedding",
        "k": 42,
    }


def test_query_hybrid_search_omits_optional_when_absent():
    h = TableQuery("t").hybrid_search("hello", [1.0], limit=3)
    out = h.build().model_dump(by_alias=True, mode="json")["hybridSearch"]
    assert "searchIndex" not in out
    assert "vectorIndex" not in out
    assert "k" not in out


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


def test_parse_result_aggregate_scalar_and_groups():
    # Aggregate scalar result is a bare JSON value (server QueryResult::Aggregate).
    # null (over an empty matching set) round-trips as None.
    assert parse_result(float, "aggregate", 42.0) == 42.0
    assert parse_result(object, "aggregate", "backlog") == "backlog"
    assert parse_result(object, "aggregate", None) is None
    # AggregateGroups: always returns list[AggregateGroup] (the {key, value}
    # shape is fixed; `model` is ignored for this terminal).
    from par_rt_db.wire import AggregateGroup

    rows = parse_result(dict, "aggregateGroups", [{"key": "backlog", "value": 4.0}])
    assert rows == [AggregateGroup(key="backlog", value=4.0)]
    assert rows[0].key == "backlog"
    assert rows[0].value == 4.0


def test_parse_result_dict_model_returns_raw_dicts():
    out = parse_result(dict, "collect", [{"id": "1"}])
    assert out == [{"id": "1"}]


def test_dump_query_serializes_tablequery_to_wire_dict():
    q = TableQuery("items").with_index("by_x").eq(1).take(5)
    d = _dump_query(q)
    assert d["table"] == "items"
    assert d["index"] == "by_x"
    assert d["eq"] == [1]
    assert d["take"] == 5


def test_terminal_of_infers_collect_by_default():
    assert _terminal_of(Query(table="items")) == "collect"
    assert _terminal_of(Query(table="items", count=True)) == "count"
    assert _terminal_of(Query(table="items", get="i1")) == "get"
