"""QA-001 / QA-002 cross-client combination-matrix safety net (Python mirror).

Mirrors ``ts-client/tests/query_combinations.test.ts`` and
``server/tests/query_combinations.rs`` case-for-case. All three files run the
SAME matrix against their respective query implementations and must agree on
every accept/reject. Adding a new terminal? Add cases here AND in the ts/server
mirrors — the matrix exists so the next terminal addition fails the gate on
whichever side forgets (this is exactly the drift class that produced QA-001:
the TS ``get`` guard omitted ``filter``/``search``/``vectorSearch``).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import pytest

from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.query import Query
from par_rt_db.schema import Schema, t

ID = "0123456789abcdef0123456789abcdef"


# --- shared payload builders (mirror the ts helper functions) ---


def _filter_eq_title_x() -> dict[str, Any]:
    return {"op": "eq", "field": "title", "value": "x"}


def _search_body_x() -> dict[str, Any]:
    return {"index": "search_body", "query": "x"}


def _vector_embedding_limit1() -> dict[str, Any]:
    return {"index": "by_embedding", "vector": [0, 0, 0], "limit": 1}


def _hybrid_query_x() -> dict[str, Any]:
    return {"query": "x", "vector": [0, 0, 0], "limit": 1}


def _paginate_num1() -> dict[str, Any]:
    return {"numItems": 1}


@dataclass(frozen=True)
class Case:
    """One matrix case: a ``build`` function that mutates a base ``{table:"items"}``
    query dict, and the expected outcome (accept or reject with BAD_REQUEST)."""

    name: str
    build: Callable[[dict[str, Any]], Any]
    expected: Literal["accept", "reject"]


# --- schema (mirror ts lines 22-33) ---


def _build_schema() -> Any:
    def table_fn(tb: Any) -> None:
        tb.field("title", t.string())
        tb.field("body", t.string())
        tb.field("count", t.number())
        tb.field("embedding", t.vector(3))
        tb.index("by_title", ["title"])
        tb.index("by_title_count", ["title", "count"])
        tb.search_index("search_body", ["title", "body"])
        tb.vector_index("by_embedding", "embedding", 3)

    return Schema.builder().table("items", table_fn).build()


def _new_client() -> InMemoryRtDbClient:
    c = InMemoryRtDbClient(
        InMemoryRtDbClientOptions(now=lambda: 1_700_000_000_000, random=lambda: 0.0)
    )
    c.push_schema(_build_schema())
    return c


def _base_query() -> dict[str, Any]:
    return {"table": "items"}


# ============================================================================
# CASES — ported case-for-case from ts-client/tests/query_combinations.test.ts
# ============================================================================

CASES: list[Case] = [
    # ============ Solo accepts (each terminal alone is valid baseline) ============
    Case(
        "solo: get",
        lambda q: q.__setitem__("get", ID),
        "accept",
    ),
    Case(
        "solo: collect",
        lambda q: None,
        "accept",
    ),
    Case(
        "solo: index",
        lambda q: q.__setitem__("index", "by_title"),
        "accept",
    ),
    Case(
        "solo: eq",
        lambda q: (q.__setitem__("index", "by_title"), q.__setitem__("eq", ["x"])),
        "accept",
    ),
    Case(
        "solo: gt",
        lambda q: (q.__setitem__("index", "by_title"), q.__setitem__("gt", "x")),
        "accept",
    ),
    Case(
        "solo: gte",
        lambda q: (q.__setitem__("index", "by_title"), q.__setitem__("gte", "x")),
        "accept",
    ),
    Case(
        "solo: lt",
        lambda q: (q.__setitem__("index", "by_title"), q.__setitem__("lt", "x")),
        "accept",
    ),
    Case(
        "solo: lte",
        lambda q: (q.__setitem__("index", "by_title"), q.__setitem__("lte", "x")),
        "accept",
    ),
    Case(
        "solo: order",
        lambda q: q.__setitem__("order", "asc"),
        "accept",
    ),
    Case(
        "solo: take",
        lambda q: q.__setitem__("take", 1),
        "accept",
    ),
    Case(
        "solo: unique",
        lambda q: q.__setitem__("unique", True),
        "accept",
    ),
    Case(
        "solo: first",
        lambda q: q.__setitem__("first", True),
        "accept",
    ),
    Case(
        "solo: count",
        lambda q: q.__setitem__("count", True),
        "accept",
    ),
    Case(
        "solo: distinct",
        lambda q: (q.__setitem__("distinct", True), q.__setitem__("index", "by_title")),
        "accept",
    ),
    Case(
        "solo: paginate",
        lambda q: q.__setitem__("paginate", _paginate_num1()),
        "accept",
    ),
    Case(
        "solo: filter",
        lambda q: q.__setitem__("filter", _filter_eq_title_x()),
        "accept",
    ),
    Case(
        "solo: search",
        lambda q: q.__setitem__("search", _search_body_x()),
        "accept",
    ),
    Case(
        "solo: vectorSearch",
        lambda q: q.__setitem__("vectorSearch", _vector_embedding_limit1()),
        "accept",
    ),
    Case(
        "solo: hybridSearch",
        lambda q: q.__setitem__("hybridSearch", _hybrid_query_x()),
        "accept",
    ),
    # ============ get rejects every peer (QA-001: last 3 are the drift) ============
    Case(
        "get+index",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("index", "by_title")),
        "reject",
    ),
    Case(
        "get+eq",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("eq", ["x"])),
        "reject",
    ),
    Case(
        "get+gt",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("gt", "x")),
        "reject",
    ),
    Case(
        "get+gte",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("gte", "x")),
        "reject",
    ),
    Case(
        "get+lt",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("lt", "x")),
        "reject",
    ),
    Case(
        "get+lte",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("lte", "x")),
        "reject",
    ),
    Case(
        "get+order",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("order", "asc")),
        "reject",
    ),
    Case(
        "get+take",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("take", 1)),
        "reject",
    ),
    Case(
        "get+unique",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("unique", True)),
        "reject",
    ),
    Case(
        "get+first",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("first", True)),
        "reject",
    ),
    Case(
        "get+count",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("count", True)),
        "reject",
    ),
    Case(
        "get+paginate",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("paginate", _paginate_num1())),
        "reject",
    ),
    Case(
        "get+filter",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("filter", _filter_eq_title_x())),
        "reject",
    ),
    Case(
        "get+search",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("search", _search_body_x())),
        "reject",
    ),
    Case(
        "get+vectorSearch",
        lambda q: (
            q.__setitem__("get", ID),
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
        ),
        "reject",
    ),
    Case(
        "get+hybridSearch",
        lambda q: (q.__setitem__("get", ID), q.__setitem__("hybridSearch", _hybrid_query_x())),
        "reject",
    ),
    # ============ unique rejects take, order ============
    Case(
        "unique+take",
        lambda q: (q.__setitem__("unique", True), q.__setitem__("take", 1)),
        "reject",
    ),
    Case(
        "unique+order",
        lambda q: (q.__setitem__("unique", True), q.__setitem__("order", "asc")),
        "reject",
    ),
    # ============ first rejects unique, take ============
    Case(
        "first+unique",
        lambda q: (q.__setitem__("first", True), q.__setitem__("unique", True)),
        "reject",
    ),
    Case(
        "first+take",
        lambda q: (q.__setitem__("first", True), q.__setitem__("take", 1)),
        "reject",
    ),
    # ============ count rejects unique, take, first, order, distinct ============
    Case(
        "count+unique",
        lambda q: (q.__setitem__("count", True), q.__setitem__("unique", True)),
        "reject",
    ),
    Case(
        "count+take",
        lambda q: (q.__setitem__("count", True), q.__setitem__("take", 1)),
        "reject",
    ),
    Case(
        "count+first",
        lambda q: (q.__setitem__("count", True), q.__setitem__("first", True)),
        "reject",
    ),
    Case(
        "count+order",
        lambda q: (q.__setitem__("count", True), q.__setitem__("order", "asc")),
        "reject",
    ),
    Case(
        "count+distinct",
        lambda q: (q.__setitem__("count", True), q.__setitem__("distinct", True)),
        "reject",
    ),
    # ============ distinct rejects get, take, unique, first, count, order,
    #              paginate, search, vectorSearch (standalone terminal like count)
    # ============
    Case(
        "distinct+get",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("get", ID),
        ),
        "reject",
    ),
    Case(
        "distinct+take",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("take", 1),
        ),
        "reject",
    ),
    Case(
        "distinct+unique",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("unique", True),
        ),
        "reject",
    ),
    Case(
        "distinct+first",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("first", True),
        ),
        "reject",
    ),
    Case(
        "distinct+count",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("count", True),
        ),
        "reject",
    ),
    Case(
        "distinct+order",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("order", "asc"),
        ),
        "reject",
    ),
    Case(
        "distinct+paginate",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("paginate", _paginate_num1()),
        ),
        "reject",
    ),
    Case(
        "distinct+search",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("search", _search_body_x()),
        ),
        "reject",
    ),
    Case(
        "distinct+vectorSearch",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
        ),
        "reject",
    ),
    Case(
        "distinct+hybridSearch",
        lambda q: (
            q.__setitem__("distinct", True),
            q.__setitem__("index", "by_title"),
            q.__setitem__("hybridSearch", _hybrid_query_x()),
        ),
        "reject",
    ),
    # ============ aggregate rejects get, take, unique, first, count, distinct,
    #              order, paginate, search, vectorSearch (standalone terminal
    #              like count/distinct); composes with index/eq/range/filter
    # ============
    Case(
        "solo: aggregate",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
        ),
        "accept",
    ),
    Case(
        "aggregate+get",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("get", ID),
        ),
        "reject",
    ),
    Case(
        "aggregate+take",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("take", 1),
        ),
        "reject",
    ),
    Case(
        "aggregate+unique",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("unique", True),
        ),
        "reject",
    ),
    Case(
        "aggregate+first",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("first", True),
        ),
        "reject",
    ),
    Case(
        "aggregate+count",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("count", True),
        ),
        "reject",
    ),
    Case(
        "aggregate+distinct",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("distinct", True),
        ),
        "reject",
    ),
    Case(
        "aggregate+order",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("order", "asc"),
        ),
        "reject",
    ),
    Case(
        "aggregate+paginate",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("paginate", _paginate_num1()),
        ),
        "reject",
    ),
    Case(
        "aggregate+search",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("search", _search_body_x()),
        ),
        "reject",
    ),
    Case(
        "aggregate+vectorSearch",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
        ),
        "reject",
    ),
    Case(
        "aggregate+hybridSearch",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("hybridSearch", _hybrid_query_x()),
        ),
        "reject",
    ),
    Case(
        "compose: aggregate+eq",
        lambda q: (
            q.__setitem__("aggregate", {"op": "sum"}),
            q.__setitem__("index", "by_title_count"),
            q.__setitem__("eq", ["x"]),
        ),
        "accept",
    ),
    Case(
        "compose: aggregate+filter",
        lambda q: (
            q.__setitem__("aggregate", {"op": "min"}),
            q.__setitem__("index", "by_title"),
            q.__setitem__("filter", _filter_eq_title_x()),
        ),
        "accept",
    ),
    # ============ paginate rejects count, unique, first, take (get covered above)
    # ============
    Case(
        "paginate+count",
        lambda q: (
            q.__setitem__("paginate", _paginate_num1()),
            q.__setitem__("count", True),
        ),
        "reject",
    ),
    Case(
        "paginate+unique",
        lambda q: (
            q.__setitem__("paginate", _paginate_num1()),
            q.__setitem__("unique", True),
        ),
        "reject",
    ),
    Case(
        "paginate+first",
        lambda q: (
            q.__setitem__("paginate", _paginate_num1()),
            q.__setitem__("first", True),
        ),
        "reject",
    ),
    Case(
        "paginate+take",
        lambda q: (
            q.__setitem__("paginate", _paginate_num1()),
            q.__setitem__("take", 1),
        ),
        "reject",
    ),
    # ============ range-bound incompatibilities ============
    Case(
        "gt+gte",
        lambda q: (
            q.__setitem__("index", "by_title"),
            q.__setitem__("gt", "x"),
            q.__setitem__("gte", "x"),
        ),
        "reject",
    ),
    Case(
        "lt+lte",
        lambda q: (
            q.__setitem__("index", "by_title"),
            q.__setitem__("lt", "x"),
            q.__setitem__("lte", "x"),
        ),
        "reject",
    ),
    # ============ vectorSearch rejects every peer (take included) ============
    Case(
        "vectorSearch+index",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("index", "by_title"),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+eq",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("eq", ["x"]),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+gt",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("gt", "x"),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+gte",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("gte", "x"),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+lt",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("lt", "x"),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+lte",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("lte", "x"),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+order",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("order", "asc"),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+unique",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("unique", True),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+first",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("first", True),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+count",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("count", True),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+paginate",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("paginate", _paginate_num1()),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+filter",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("filter", _filter_eq_title_x()),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+search",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("search", _search_body_x()),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+take",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("take", 1),
        ),
        "reject",
    ),
    Case(
        "vectorSearch+hybridSearch",
        lambda q: (
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
            q.__setitem__("hybridSearch", _hybrid_query_x()),
        ),
        "reject",
    ),
    # ============ search rejects every peer except take ============
    Case(
        "search+index",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("index", "by_title"),
        ),
        "reject",
    ),
    Case(
        "search+eq",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("eq", ["x"]),
        ),
        "reject",
    ),
    Case(
        "search+gt",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("gt", "x"),
        ),
        "reject",
    ),
    Case(
        "search+gte",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("gte", "x"),
        ),
        "reject",
    ),
    Case(
        "search+lt",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("lt", "x"),
        ),
        "reject",
    ),
    Case(
        "search+lte",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("lte", "x"),
        ),
        "reject",
    ),
    Case(
        "search+order",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("order", "asc"),
        ),
        "reject",
    ),
    Case(
        "search+unique",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("unique", True),
        ),
        "reject",
    ),
    Case(
        "search+first",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("first", True),
        ),
        "reject",
    ),
    Case(
        "search+count",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("count", True),
        ),
        "reject",
    ),
    Case(
        "search+paginate",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("paginate", _paginate_num1()),
        ),
        "reject",
    ),
    Case(
        "search+filter",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("filter", _filter_eq_title_x()),
        ),
        "reject",
    ),
    Case(
        "search+vectorSearch",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("vectorSearch", _vector_embedding_limit1()),
        ),
        "reject",
    ),
    Case(
        "search+hybridSearch",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("hybridSearch", _hybrid_query_x()),
        ),
        "reject",
    ),
    # ============ hybridSearch rejects every peer (standalone, like vectorSearch)
    # ============
    Case(
        "hybridSearch+index",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("index", "by_title"),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+eq",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("eq", ["x"]),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+gt",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("gt", "x"),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+gte",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("gte", "x"),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+lt",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("lt", "x"),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+lte",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("lte", "x"),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+order",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("order", "asc"),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+unique",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("unique", True),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+first",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("first", True),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+count",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("count", True),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+distinct",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("distinct", True),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+aggregate",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("aggregate", {"op": "min"}),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+paginate",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("paginate", _paginate_num1()),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+filter",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("filter", _filter_eq_title_x()),
        ),
        "reject",
    ),
    Case(
        "hybridSearch+take",
        lambda q: (
            q.__setitem__("hybridSearch", _hybrid_query_x()),
            q.__setitem__("take", 1),
        ),
        "reject",
    ),
    # ============ composition accepts (smoke that valid combos don't false-reject)
    # ============
    Case(
        "compose: search+take",
        lambda q: (
            q.__setitem__("search", _search_body_x()),
            q.__setitem__("take", 1),
        ),
        "accept",
    ),
    Case(
        "compose: index+take",
        lambda q: (
            q.__setitem__("index", "by_title"),
            q.__setitem__("take", 1),
        ),
        "accept",
    ),
    Case(
        "compose: index+eq+take",
        lambda q: (
            q.__setitem__("index", "by_title"),
            q.__setitem__("eq", ["x"]),
            q.__setitem__("take", 1),
        ),
        "accept",
    ),
    Case(
        "compose: index+order",
        lambda q: (
            q.__setitem__("index", "by_title"),
            q.__setitem__("order", "asc"),
        ),
        "accept",
    ),
    Case(
        "compose: index+gt+lt",
        lambda q: (
            q.__setitem__("index", "by_title"),
            q.__setitem__("gt", "a"),
            q.__setitem__("lt", "z"),
        ),
        "accept",
    ),
    Case(
        "compose: take+filter",
        lambda q: (
            q.__setitem__("take", 1),
            q.__setitem__("filter", _filter_eq_title_x()),
        ),
        "accept",
    ),
]


# --- test runner ---


_CASE_BY_NAME = {c.name: c for c in CASES}


def _run_case(case: Case) -> None:
    """Build the query, run it, and assert the expected outcome.

    A Reject is an :class:`RtDbError` with code ``BAD_REQUEST`` raised from
    :meth:`run_query`. Any other error re-raises (signals an unexpected engine
    fault, not a matrix classification). Pydantic ``ValidationError`` from
    ``Query.model_validate`` also propagates — it signals a test-construction
    issue, not the engine's rejection cascade.
    """
    client = _new_client()
    q_dict = _base_query()
    case.build(q_dict)
    query = Query.model_validate(q_dict)
    try:
        client.run_query(query)
    except RtDbError as err:
        if err.code is ErrorCode.BAD_REQUEST:
            assert case.expected == "reject", (
                f"{case.name}: unexpectedly rejected with BAD_REQUEST: {err.message}"
            )
            return
        raise
    assert case.expected == "accept", (
        f"{case.name}: expected BAD_REQUEST rejection but query was accepted"
    )


@pytest.mark.parametrize("case_name", [c.name for c in CASES], ids=lambda v: v)
def test_combination_matrix(case_name: str) -> None:
    """Run one matrix case (parametrized for per-case failure granularity)."""
    _run_case(_CASE_BY_NAME[case_name])


def test_matrix_covers_qa001_drift_cases() -> None:
    """Regression guard: the three QA-001 drift cases must be present and Reject.

    The TS ``get`` guard used to omit ``filter``/``search``/``vectorSearch`` and
    silently accepted them. If any of these cases are removed or reclassified,
    fail loudly — they are the load-bearing regression cases for the QA-001 fix.
    """
    names = {c.name for c in CASES}
    for drift_case in ("get+filter", "get+search", "get+vectorSearch"):
        assert drift_case in names, f"matrix lost QA-001 drift case: {drift_case}"
        assert _CASE_BY_NAME[drift_case].expected == "reject", (
            f"QA-001 drift case {drift_case} must be Reject, got accept"
        )
