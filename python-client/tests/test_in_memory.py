"""Tests for :mod:`par_rt_db.in_memory` — the offline test harness.

Ports a representative slice of ``rust-client/src/in_memory.rs``'s test suite:
schema push (+ destructive-change rejection), insert→collect with system-field
merge, index eq/range filtering, order+take, the full mutate op set
(patch/replace/delete/upsert + expectVersion + txn rollback + mut_id
idempotency), reactive subscribe (initial value, update-on-change, unsubscribe),
and ``tick`` firing a due scheduled job (and skipping not-yet-due / paused jobs).
"""

from __future__ import annotations

from typing import Any, Literal

import pytest
from pydantic import TypeAdapter

from par_rt_db import Mutation, StepResult, TableQuery
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import (
    InMemoryRtDbClient,
    InMemoryRtDbClientOptions,
    is_hex_id,
)
from par_rt_db.query import Query
from par_rt_db.schema import Schema, t
from par_rt_db.wire import AggregateSpec, ScheduleWhen

_when = TypeAdapter(ScheduleWhen)


def _inserted(res: StepResult) -> bool:
    """``True`` iff ``res`` is an upsert result with ``inserted=True``."""
    assert res is not None
    return bool(res.model_dump().get("inserted"))


def _test_schema() -> Any:
    """The ``items`` schema mirrored from the Rust/TS harnesses."""
    return (
        Schema.builder()
        .table(
            "items",
            lambda tb: (
                tb.field("name", t.string())
                .field("status", t.string())
                .field("order", t.number())
                .field("note", t.optional(t.string()))
                .index("by_name", ["name"])
                .index("by_status", ["status"])
                .index("by_status_and_order", ["status", "order"])
            ),
        )
        .build()
    )


def _new_client() -> InMemoryRtDbClient:
    # Post-incrementing epoch-millis clock + constant 0.0 RNG, mirroring the
    # Rust/TS `newClient` fixture so each insert mints a distinct `_id`.
    counter = [1_700_000_000_000]

    def now() -> int:
        v = counter[0]
        counter[0] += 1
        return v

    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.0))
    c.push_schema(_test_schema())
    return c


# ---------------------------------------------------------------------------
# schema push
# ---------------------------------------------------------------------------


def test_push_schema_stores_the_schema() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_test_schema())
    stored = c.to_schema_json()
    assert stored is not None
    assert "items" in stored.tables


def test_push_schema_rejects_a_destructive_second_push() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_test_schema())
    only_other = Schema.builder().table("solo", lambda tb: tb.field("x", t.number())).build()
    with pytest.raises(RtDbError) as ei:
        c.push_schema(only_other)
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "removed table 'items'" in ei.value.message
    # The rejected push left the prior schema in place.
    assert "items" in c.to_schema_json().tables  # type: ignore[union-attr]
    assert "solo" not in c.to_schema_json().tables  # type: ignore[union-attr]


def test_push_schema_additively_preserves_docs() -> None:
    c = _new_client()
    c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    )
    additive = (
        Schema.builder()
        .table(
            "items",
            lambda tb: (
                tb.field("name", t.string())
                .field("status", t.string())
                .field("order", t.number())
                .field("note", t.optional(t.string()))
                .field("priority", t.optional(t.number()))
                .index("by_name", ["name"])
                .index("by_status", ["status"])
                .index("by_status_and_order", ["status", "order"])
            ),
        )
        .table("users", lambda tb: tb.field("email", t.string()))
        .build()
    )
    c.push_schema(additive)
    stored = c.to_schema_json()
    assert stored is not None
    assert "users" in stored.tables
    assert "priority" in stored.tables["items"].fields
    # Pre-existing row still queryable.
    docs = c.run_query(TableQuery("items").build())
    assert len(docs) == 1


def _field_schema(field_type: Any) -> Any:
    """One-table, one-field schema used by the literal-widening parity tests."""
    return Schema.builder().table("things", lambda tb: tb.field("val", field_type)).build()


def test_push_schema_widens_literal_union_by_adding_a_variant() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_field_schema(t.union([t.literal("a"), t.literal("b")])))
    # {a,b} -> {a,b,c} is additive widening, must succeed.
    c.push_schema(_field_schema(t.union([t.literal("a"), t.literal("b"), t.literal("c")])))


def test_push_schema_widens_single_literal_to_union() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_field_schema(t.literal("a")))
    # "a" -> {a,b} is additive widening, must succeed.
    c.push_schema(_field_schema(t.union([t.literal("a"), t.literal("b")])))


def test_push_schema_rejects_narrowing_a_union() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_field_schema(t.union([t.literal("a"), t.literal("b"), t.literal("c")])))
    with pytest.raises(RtDbError) as ei:
        c.push_schema(_field_schema(t.union([t.literal("a"), t.literal("b")])))
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "changed type of field 'things.val'" in ei.value.message


def test_push_schema_rejects_replacing_one_literal_with_another() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_field_schema(t.literal("a")))
    with pytest.raises(RtDbError) as ei:
        c.push_schema(_field_schema(t.literal("b")))
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "changed type of field 'things.val'" in ei.value.message


def test_push_schema_rejects_collapsing_a_union_to_a_literal() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_field_schema(t.union([t.literal("a"), t.literal("b")])))
    with pytest.raises(RtDbError) as ei:
        c.push_schema(_field_schema(t.literal("a")))
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "changed type of field 'things.val'" in ei.value.message


def test_push_schema_rejects_a_non_literal_type_change() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_field_schema(t.string()))
    with pytest.raises(RtDbError) as ei:
        c.push_schema(_field_schema(t.number()))
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "changed type of field 'things.val'" in ei.value.message


def test_push_schema_rejects_widening_to_an_empty_union() -> None:
    c = InMemoryRtDbClient()
    c.push_schema(_field_schema(t.literal("a")))
    with pytest.raises(RtDbError) as ei:
        c.push_schema(_field_schema(t.union([])))
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "changed type of field 'things.val'" in ei.value.message


# ---------------------------------------------------------------------------
# insert / collect / system fields
# ---------------------------------------------------------------------------


def test_insert_merges_system_fields_at_read_time() -> None:
    c = _new_client()
    [res] = c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    )
    assert res is not None
    assert is_hex_id(res.id)
    doc = c.run_query(TableQuery("items").get(res.id).build())
    assert doc["name"] == "a"
    assert doc["status"] == "todo"
    assert doc["order"] == 1
    assert doc["_id"] == res.id
    assert doc["_version"] == 1
    assert isinstance(doc["_creationTime"], int)


def test_insert_strips_optional_field_set_to_null() -> None:
    c = _new_client()
    [res] = c.mutate(
        Mutation.builder()
        .insert("items", {"name": "a", "status": "todo", "order": 1, "note": None})
        .build()
    )
    assert res is not None
    doc = c.run_query(TableQuery("items").get(res.id).build())
    assert "note" not in doc  # null optional is stored as "key absent"


def test_insert_rejects_missing_required_field() -> None:
    c = _new_client()
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().insert("items", {"name": "a"}).build())
    assert ei.value.code is ErrorCode.SCHEMA_VIOLATION
    assert "status" in ei.value.message  # required field reported


# ---------------------------------------------------------------------------
# queries: index eq / range / order / take / count / unique
# ---------------------------------------------------------------------------


def _seed_query_rows(c: InMemoryRtDbClient) -> list[str]:
    ids: list[str] = []
    for name, status, order in [("a", "todo", 2), ("b", "todo", 1), ("c", "done", 3)]:
        [res] = c.mutate(
            Mutation.builder()
            .insert("items", {"name": name, "status": status, "order": order})
            .build()
        )
        assert res is not None
        ids.append(res.id)
    return ids


def test_index_eq_returns_only_matching_rows() -> None:
    c = _new_client()
    _seed_query_rows(c)
    docs = c.run_query(TableQuery("items").with_index("by_status").eq("todo").build())
    assert {d["name"] for d in docs} == {"a", "b"}


def test_index_range_with_eq_prefix_orders_numerically() -> None:
    c = _new_client()
    _seed_query_rows(c)
    docs = c.run_query(
        TableQuery("items").with_index("by_status_and_order").eq("todo").order("asc").build()
    )
    # Composite index (status, order): eq on status, sort by order ascending.
    assert [d["order"] for d in docs] == [1, 2]
    docs_desc = c.run_query(
        TableQuery("items").with_index("by_status_and_order").eq("todo").order("desc").build()
    )
    assert [d["order"] for d in docs_desc] == [2, 1]


def test_index_range_bound_filters_a_window() -> None:
    c = _new_client()
    _seed_query_rows(c)
    docs = c.run_query(
        TableQuery("items").with_index("by_status_and_order").eq("todo").gte(2).build()
    )
    assert [d["order"] for d in docs] == [2]


def test_take_limits_the_result_set() -> None:
    c = _new_client()
    _seed_query_rows(c)
    docs = c.run_query(TableQuery("items").with_index("by_status").eq("todo").take(1).build())
    assert len(docs) == 1


def test_count_returns_cardinality_of_filtered_set() -> None:
    c = _new_client()
    _seed_query_rows(c)
    n = c.run_query(TableQuery("items").with_index("by_status").eq("todo").count().build())
    assert n == 2


def test_unique_returns_the_single_match() -> None:
    c = _new_client()
    _seed_query_rows(c)
    doc = c.run_query(TableQuery("items").with_index("by_name").eq("a").unique().build())
    assert doc is not None
    assert doc["name"] == "a"


def test_unique_raises_when_multiple_match() -> None:
    c = _new_client()
    _seed_query_rows(c)
    with pytest.raises(RtDbError) as ei:
        c.run_query(TableQuery("items").with_index("by_status").eq("todo").unique().build())
    assert ei.value.code is ErrorCode.PRECONDITION_FAILED


def test_filter_reduces_the_result_set() -> None:
    from par_rt_db.wire import FilterExpr

    c = _new_client()
    _seed_query_rows(c)
    flt = TypeAdapter(FilterExpr).validate_python({"op": "gt", "field": "order", "value": 1})
    docs = c.run_query(TableQuery("items").filter(flt).build())
    assert {d["name"] for d in docs} == {"a", "c"}


def test_paginate_keyset_pages_through_the_sorted_set() -> None:
    c = _new_client()
    _seed_query_rows(c)  # todo: order 2,1 ; done: order 3
    q = (
        TableQuery("items")
        .with_index("by_status_and_order")
        .eq("todo")
        .order("asc")
        .paginate(num_items=1)
    )
    page1 = c.run_query(q.build())
    assert [d["order"] for d in page1["docs"]] == [1]
    assert page1.get("nextCursor") is not None
    page2 = c.run_query(q.paginate(cursor=page1["nextCursor"], num_items=1).build())
    assert [d["order"] for d in page2["docs"]] == [2]
    # No more todo rows after the second page.
    assert page2.get("nextCursor") is None


def test_run_returns_typed_results_via_parse_result() -> None:
    from par_rt_db.query import Paginated

    c = _new_client()
    _seed_query_rows(c)
    docs = c.run(TableQuery("items").with_index("by_status").eq("todo").build(), model=dict)
    assert isinstance(docs, list)
    assert {d["name"] for d in docs} == {"a", "b"}
    n = c.run(TableQuery("items").count().build())
    assert n == 3
    page = c.run(
        TableQuery("items").with_index("by_status").eq("todo").paginate(num_items=10).build()
    )
    assert isinstance(page, Paginated)
    assert len(page.docs) == 2
    assert page.next_cursor is None


# ---------------------------------------------------------------------------
# queries: distinct / aggregate terminals
# ---------------------------------------------------------------------------


def _seed_orders(c: InMemoryRtDbClient, orders: list[int], status: str = "todo") -> None:
    for order in orders:
        c.mutate(
            Mutation.builder()
            .insert("items", {"name": f"n{order}", "status": status, "order": order})
            .build()
        )


def test_distinct_returns_unique_index_field_values_sorted_asc() -> None:
    c = _new_client()
    _seed_orders(c, [3, 1, 2])
    v = c.run_query(
        TableQuery("items").with_index("by_status_and_order").eq("todo").distinct().build()
    )
    assert v == [1, 2, 3]


def test_distinct_dedupes_repeated_values() -> None:
    c = _new_client()
    _seed_orders(c, [3, 1, 2, 1, 2])
    v = c.run_query(
        TableQuery("items").with_index("by_status_and_order").eq("todo").distinct().build()
    )
    assert v == [1, 2, 3]


def test_distinct_composes_with_range_bound() -> None:
    c = _new_client()
    _seed_orders(c, [3, 1, 2])
    v = c.run_query(
        TableQuery("items").with_index("by_status_and_order").eq("todo").gte(2).distinct().build()
    )
    assert v == [2, 3]


def test_distinct_empty_matching_set_returns_empty_list() -> None:
    c = _new_client()
    _seed_orders(c, [3, 1, 2])
    v = c.run_query(
        TableQuery("items").with_index("by_status_and_order").eq("missing").distinct().build()
    )
    assert v == []


def test_distinct_requires_an_index_field_beyond_eq_prefix() -> None:
    c = _new_client()
    with pytest.raises(RtDbError) as ei:
        c.run_query(
            TableQuery("items").with_index("by_status_and_order").eq("todo", 1).distinct().build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "distinct requires an index field beyond the eq prefix" in ei.value.message


def test_distinct_rejects_conflicting_terminals() -> None:
    # Ownership mirrors the server's check order: get/unique/first/count are
    # validated before distinct, so distinct+{get,unique,first,count} surfaces
    # that terminal's message; distinct owns take/order/aggregate.
    c = _new_client()

    def base(**kw: Any) -> Query:
        return Query(table="items", index="by_status_and_order", eq=["todo"], **kw)

    sum_spec = AggregateSpec(op="sum")
    cases: list[tuple[Query, str]] = [
        (base(distinct=True, take=1), "distinct cannot be combined with take"),
        (base(distinct=True, order="asc"), "distinct cannot be combined with order"),
        (base(distinct=True, aggregate=sum_spec), "distinct cannot be combined with aggregate"),
        (
            base(distinct=True, unique=True),
            "unique cannot be combined with take, order, distinct, or aggregate",
        ),
        (base(distinct=True, first=True), "first cannot be combined with distinct"),
        (base(distinct=True, count=True), "count cannot be combined with distinct"),
        (base(distinct=True, get="x"), "get cannot be combined with"),
    ]
    for q, needle in cases:
        with pytest.raises(RtDbError) as ei:
            c.run_query(q)
        assert ei.value.code is ErrorCode.BAD_REQUEST, f"case {needle!r}: {ei.value.message}"
        assert needle in ei.value.message, f"case {needle!r}: got {ei.value.message}"


def test_aggregate_sum_avg_min_max_over_numeric_field() -> None:
    c = _new_client()
    _seed_orders(c, [3, 1, 2])

    def agg(op: Literal["sum", "avg", "min", "max"]) -> Any:
        return c.run_query(
            TableQuery("items").with_index("by_status_and_order").eq("todo").aggregate(op).build()
        )

    assert agg("sum") == 6
    assert agg("avg") == 2.0
    assert agg("min") == 1
    assert agg("max") == 3


def test_aggregate_empty_matching_set_returns_none() -> None:
    c = _new_client()
    _seed_orders(c, [3, 1, 2])
    v = c.run_query(
        TableQuery("items").with_index("by_status_and_order").eq("missing").aggregate("sum").build()
    )
    assert v is None


def test_aggregate_sum_requires_a_numeric_field() -> None:
    c = _new_client()
    _seed_orders(c, [1])
    with pytest.raises(RtDbError) as ei:
        c.run_query(TableQuery("items").with_index("by_status").aggregate("sum").build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "aggregate op sum requires a numeric index field" in ei.value.message


def test_aggregate_min_max_over_string_field_are_lexicographic() -> None:
    c = _new_client()
    for i, status in enumerate(["charlie", "alpha", "bravo"]):
        c.mutate(
            Mutation.builder().insert("items", {"name": "n", "status": status, "order": i}).build()
        )
    idx = TableQuery("items").with_index("by_status")
    assert c.run_query(idx.aggregate("min").build()) == "alpha"
    assert c.run_query(idx.aggregate("max").build()) == "charlie"


def test_aggregate_group_by_groups_and_aggregates() -> None:
    c = _new_client()
    for status, order in [("todo", 1), ("todo", 2), ("done", 3), ("done", 4)]:
        c.mutate(
            Mutation.builder()
            .insert("items", {"name": "n", "status": status, "order": order})
            .build()
        )
    v = c.run_query(
        TableQuery("items")
        .with_index("by_status_and_order")
        .aggregate("sum", group_by=True)
        .build()
    )
    assert v == [{"key": "done", "value": 7}, {"key": "todo", "value": 3}]


def test_aggregate_group_by_requires_two_index_fields_beyond_prefix() -> None:
    c = _new_client()
    with pytest.raises(RtDbError) as ei:
        c.run_query(
            TableQuery("items").with_index("by_status").aggregate("sum", group_by=True).build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "aggregate groupBy requires two index fields beyond the eq prefix" in ei.value.message


def test_aggregate_requires_an_index_field_beyond_eq_prefix() -> None:
    c = _new_client()
    with pytest.raises(RtDbError) as ei:
        c.run_query(
            TableQuery("items")
            .with_index("by_status_and_order")
            .eq("todo", 1)
            .aggregate("min")
            .build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "aggregate requires an index field beyond the eq prefix" in ei.value.message


def test_aggregate_rejects_conflicting_terminals() -> None:
    c = _new_client()

    def base(**kw: Any) -> Query:
        return Query(table="items", index="by_status_and_order", eq=["todo"], **kw)

    sum_spec = AggregateSpec(op="sum")
    cases: list[tuple[Query, str]] = [
        (base(aggregate=sum_spec, take=1), "aggregate cannot be combined with take"),
        (base(aggregate=sum_spec, order="asc"), "aggregate cannot be combined with order"),
        (
            base(aggregate=sum_spec, unique=True),
            "unique cannot be combined with take, order, distinct, or aggregate",
        ),
        (base(aggregate=sum_spec, first=True), "first cannot be combined with aggregate"),
        (base(aggregate=sum_spec, count=True), "count cannot be combined with aggregate"),
        (base(aggregate=sum_spec, distinct=True), "distinct cannot be combined with aggregate"),
        (base(aggregate=sum_spec, get="x"), "get cannot be combined with"),
    ]
    for q, needle in cases:
        with pytest.raises(RtDbError) as ei:
            c.run_query(q)
        assert ei.value.code is ErrorCode.BAD_REQUEST, f"case {needle!r}: {ei.value.message}"
        assert needle in ei.value.message, f"case {needle!r}: got {ei.value.message}"


# ---------------------------------------------------------------------------
# mutations: patch / replace / delete / upsert / expectVersion
# ---------------------------------------------------------------------------


def test_patch_updates_a_field_and_bumps_version() -> None:
    c = _new_client()
    [res] = c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    )
    assert res is not None
    c.mutate(Mutation.builder().patch("items", res.id, {"status": "done"}).build())
    doc = c.run_query(TableQuery("items").get(res.id).build())
    assert doc["status"] == "done"
    assert doc["_version"] == 2


def test_replace_overwrites_the_whole_doc() -> None:
    c = _new_client()
    [res] = c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    )
    assert res is not None
    c.mutate(
        Mutation.builder()
        .replace("items", res.id, {"name": "a2", "status": "done", "order": 9})
        .build()
    )
    doc = c.run_query(TableQuery("items").get(res.id).build())
    assert doc["name"] == "a2"
    assert doc["order"] == 9
    assert doc["_version"] == 2


def test_delete_removes_the_doc() -> None:
    c = _new_client()
    [res] = c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    )
    assert res is not None
    c.mutate(Mutation.builder().delete("items", res.id).build())
    assert c.run_query(TableQuery("items").get(res.id).build()) is None


def test_delete_unknown_id_is_not_found() -> None:
    c = _new_client()
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().delete("items", "deadbeef").build())
    assert ei.value.code is ErrorCode.NOT_FOUND


def test_upsert_inserts_on_no_match_and_patches_on_match() -> None:
    c = _new_client()
    [r1] = c.mutate(
        Mutation.builder()
        .upsert(
            "items",
            "by_name",
            ["a"],
            insert={"name": "a", "status": "todo", "order": 1},
            patch={"order": 5},
        )
        .build()
    )
    assert r1 is not None
    assert _inserted(r1) is True
    # Second upsert matches on by_name='a' and applies the patch.
    [r2] = c.mutate(
        Mutation.builder()
        .upsert(
            "items",
            "by_name",
            ["a"],
            insert={"name": "a", "status": "todo", "order": 1},
            patch={"order": 5},
        )
        .build()
    )
    assert r2 is not None
    assert _inserted(r2) is False
    assert r2.id == r1.id
    doc = c.run_query(TableQuery("items").get(r1.id).build())
    assert doc["order"] == 5


def test_expect_version_passes_and_fails() -> None:
    c = _new_client()
    [res] = c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    )
    assert res is not None
    # Correct version passes.
    c.mutate(Mutation.builder().expect_version("items", res.id, 1).build())
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().expect_version("items", res.id, 99).build())
    assert ei.value.code is ErrorCode.PRECONDITION_FAILED


def test_txn_rolls_back_on_later_step_failure() -> None:
    c = _new_client()
    before = c.run_query(TableQuery("items").build())
    assert before == []
    with pytest.raises(RtDbError):
        c.mutate(
            Mutation.builder()
            .insert("items", {"name": "a", "status": "todo", "order": 1})
            .delete("items", "nonexistent")  # NOT_FOUND -> rollback the insert
            .build()
        )
    assert c.run_query(TableQuery("items").build()) == []


def test_mut_id_caches_results_and_short_circuits() -> None:
    c = _new_client()
    txn = Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    r1 = c.mutate(txn, mut_id="m1")
    r2 = c.mutate(txn, mut_id="m1")
    assert r1 == r2
    # Only the first call wrote a row; the second short-circuited.
    assert len(c.run_query(TableQuery("items").build())) == 1


# ---------------------------------------------------------------------------
# subscribe
# ---------------------------------------------------------------------------


def _todo_count_query() -> Any:
    return TableQuery("items").with_index("by_status").eq("todo").count().build()


def test_subscribe_delivers_initial_value_and_recomputes_only_on_change() -> None:
    c = _new_client()
    updates: list[int] = []
    handle = c.subscribe(_todo_count_query(), lambda v: updates.append(v))
    try:
        assert updates == [0], "initial value delivered synchronously"
        c.mutate(
            Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
        )
        assert updates == [0, 1], "todo insert bumped the count"
        # A write to a different status doesn't change the todo count.
        c.mutate(
            Mutation.builder().insert("items", {"name": "b", "status": "done", "order": 2}).build()
        )
        assert updates == [0, 1], "done insert did not change the todo count"
    finally:
        handle.unsubscribe()


def test_subscribe_unsubscribe_stops_further_updates() -> None:
    c = _new_client()
    updates: list[int] = []
    handle = c.subscribe(_todo_count_query(), lambda v: updates.append(v))
    assert updates == [0]
    handle.unsubscribe()
    c.mutate(
        Mutation.builder().insert("items", {"name": "c", "status": "todo", "order": 3}).build()
    )
    assert updates == [0], "no further updates after unsubscribe"


def test_subscribe_context_manager_unsubscribes_on_exit() -> None:
    c = _new_client()
    updates: list[int] = []
    with c.subscribe(_todo_count_query(), lambda v: updates.append(v)):
        assert updates == [0]
    c.mutate(
        Mutation.builder().insert("items", {"name": "d", "status": "todo", "order": 4}).build()
    )
    assert updates == [0], "exiting the with-block cleared the listener"


# ---------------------------------------------------------------------------
# schedules + tick
# ---------------------------------------------------------------------------


def _new_clock_client() -> tuple[InMemoryRtDbClient, list[int]]:
    clock = [1_700_000_000_000]
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: clock[0], random=lambda: 0.0))
    c.push_schema(_test_schema())
    return c, clock


def _insert_todo_txn() -> Any:
    return Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()


def test_tick_fires_a_due_oneshot_and_write_is_visible() -> None:
    c, clock = _new_clock_client()
    sid = c.schedule(_insert_todo_txn(), _when.validate_python({"type": "afterMs", "ms": 1000}))
    assert is_hex_id(sid)
    clock[0] += 2000  # past the due time
    c.tick()
    docs = c.run_query(TableQuery("items").build())
    assert len(docs) == 1
    assert docs[0]["name"] == "a"
    # A fired one-shot is removed from the registry.
    assert c.list_schedules() == []


def test_tick_does_not_fire_a_not_yet_due_oneshot() -> None:
    c, _clock = _new_clock_client()
    c.schedule(_insert_todo_txn(), _when.validate_python({"type": "afterMs", "ms": 1000}))
    c.tick()  # clock is still at t0; due_at is t0+1000
    assert c.run_query(TableQuery("items").build()) == []
    assert len(c.list_schedules()) == 1


def test_tick_does_not_fire_a_paused_job() -> None:
    c, clock = _new_clock_client()
    sid = c.schedule(_insert_todo_txn(), _when.validate_python({"type": "afterMs", "ms": 1000}))
    c.pause_schedule(sid)
    clock[0] += 2000
    c.tick()
    assert c.run_query(TableQuery("items").build()) == []
    # Paused jobs remain in the registry.
    info = c.list_schedules()
    assert len(info) == 1
    assert info[0].status == "paused"


def test_pause_then_resume_lets_the_job_fire_on_a_later_tick() -> None:
    c, clock = _new_clock_client()
    sid = c.schedule(_insert_todo_txn(), _when.validate_python({"type": "afterMs", "ms": 1000}))
    c.pause_schedule(sid)
    clock[0] += 2000
    c.tick()  # paused -> no fire
    c.resume_schedule(sid)
    c.tick()  # now due and pending -> fires
    assert len(c.run_query(TableQuery("items").build())) == 1


def test_tick_cron_re_arms_and_fires_again_on_a_later_tick() -> None:
    from par_rt_db.in_memory import CRON_STEP_MS

    c, clock = _new_clock_client()
    c.schedule(_insert_todo_txn(), _when.validate_python({"type": "cron", "expr": "* * * * *"}))
    # due_at = t0 + CRON_STEP_MS; clock is still t0, so not due yet.
    c.tick()
    assert c.run_query(TableQuery("items").build()) == []
    clock[0] += CRON_STEP_MS + 1
    c.tick()
    assert len(c.run_query(TableQuery("items").build())) == 1
    # Re-armed: a later tick past the next interval fires again.
    clock[0] += CRON_STEP_MS + 1
    c.tick()
    assert len(c.run_query(TableQuery("items").build())) == 2


def test_cancel_schedule_removes_the_job() -> None:
    c, _clock = _new_clock_client()
    sid = c.schedule(_insert_todo_txn(), _when.validate_python({"type": "afterMs", "ms": 1000}))
    c.cancel_schedule(sid)
    assert c.list_schedules() == []
    with pytest.raises(RtDbError) as ei:
        c.cancel_schedule(sid)
    assert ei.value.code is ErrorCode.NOT_FOUND


# ---------------------------------------------------------------------------
# storage stubs (light coverage)
# ---------------------------------------------------------------------------


def test_upload_and_get_file_metadata_round_trip() -> None:
    c = InMemoryRtDbClient()
    res = c.upload(b"hello world", "text/plain")
    assert res.size == len(b"hello world")
    assert res.content_type == "text/plain"
    meta = c.get_file_metadata(res.id)
    assert meta.size == len(b"hello world")
    assert meta.content_type == "text/plain"
    assert c.get_url(res.id) == f"memory://{res.id}"


def test_delete_file_removes_the_blob() -> None:
    c = InMemoryRtDbClient()
    res = c.upload(b"data")
    c.delete_file(res.id)
    with pytest.raises(RtDbError) as ei:
        c.get_file_metadata(res.id)
    assert ei.value.code is ErrorCode.NOT_FOUND


# ---------------------------------------------------------------------------
# schema migration (migrate_schema)
# ---------------------------------------------------------------------------


def _migrate_client() -> InMemoryRtDbClient:
    """A client with a ``users`` table and two rows for migration tests."""
    from par_rt_db import Mutation

    schema = (
        Schema.builder()
        .table(
            "users",
            lambda tb: (
                tb.field("name", t.string())
                .field("age", t.number())
                .field("status", t.optional(t.string()))
                .index("by_name", ["name"])
            ),
        )
        .build()
    )
    # Post-incrementing epoch-millis clock so each insert mints a distinct `_id`
    # (the default constant-RNG client would collide within the same millisecond).
    counter = [1_700_000_000_000]

    def now() -> int:
        v = counter[0]
        counter[0] += 1
        return v

    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.0))
    c.push_schema(schema)
    c.mutate(Mutation.builder().insert("users", {"name": "alice", "age": 30}).build())
    c.mutate(Mutation.builder().insert("users", {"name": "bob", "age": 25}).build())
    return c


def test_migrate_rename_field_moves_doc_key_and_schema() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    directives = Migration.builder().rename_field("users", "name", "fullName").build().directives
    result = c.migrate_schema(directives)

    assert result.applied is True
    assert result.directives[0].op == "renameField"
    assert result.directives[0].affected_rows == 2  # both rows had `name`
    # Schema folded: `fullName` in, `name` out.
    table = result.schema_.tables["users"]
    assert "fullName" in table.fields
    assert "name" not in table.fields
    # Doc keys renamed.
    docs = c.collect_all("users")
    assert all("fullName" in d and "name" not in d for d in docs)


def test_migrate_set_default_populates_missing_field() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    directives = Migration.builder().set_default("users", "status", "active").build().directives
    result = c.migrate_schema(directives)

    assert result.applied is True
    assert result.directives[0].op == "setDefault"
    assert result.directives[0].affected_rows == 2  # both rows lacked `status`
    docs = c.collect_all("users")
    assert all(d["status"] == "active" for d in docs)


def test_migrate_change_type_coerces_values() -> None:
    from par_rt_db import Cast, Migration

    c = _migrate_client()
    directives = (
        Migration.builder()
        .change_type("users", "age", {"type": "string"}, Cast.TO_STRING)
        .build()
        .directives
    )
    result = c.migrate_schema(directives)

    assert result.applied is True
    assert result.directives[0].op == "changeType"
    assert result.directives[0].affected_rows == 2
    # Values coerced from number to string.
    docs = c.collect_all("users")
    assert all(isinstance(d["age"], str) for d in docs)
    # Schema updated.
    assert result.schema_.tables["users"].fields["age"].type == "string"


def test_migrate_change_type_uses_default_when_value_not_coercible() -> None:
    # Insert a row with a string value that can't coerce under ToNumber.
    from par_rt_db import Cast, Migration, Mutation

    schema = Schema.builder().table("items", lambda tb: tb.field("val", t.string())).build()
    c = InMemoryRtDbClient()
    c.push_schema(schema)
    c.mutate(Mutation.builder().insert("items", {"val": "not-a-number"}).build())

    directives = (
        Migration.builder()
        .change_type("items", "val", {"type": "number"}, Cast.TO_NUMBER, 0)
        .build()
        .directives
    )
    result = c.migrate_schema(directives)
    assert result.applied is True
    docs = c.collect_all("items")
    # The uncoercible string was replaced by the default (0 → 0.0).
    assert docs[0]["val"] == 0.0


def test_migrate_change_type_fails_without_default_when_not_coercible() -> None:
    from par_rt_db import Cast, Migration, Mutation

    schema = Schema.builder().table("items", lambda tb: tb.field("val", t.string())).build()
    c = InMemoryRtDbClient()
    c.push_schema(schema)
    c.mutate(Mutation.builder().insert("items", {"val": "abc"}).build())

    directives = (
        Migration.builder()
        .change_type("items", "val", {"type": "number"}, Cast.TO_NUMBER)
        .build()
        .directives
    )
    with pytest.raises(RtDbError) as ei:
        c.migrate_schema(directives)
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "no default given" in ei.value.message
    # Docs restored on failure.
    assert c.collect_all("items")[0]["val"] == "abc"


def test_coerce_value_to_int64_bounds_i64_range() -> None:
    # ``_coerce_value`` mirrors ``server::migrate::coerce_value``, which rejects
    # values outside i64 range (``i64::from_str`` / ``Number::as_i64``). Python
    # ints are arbitrary-precision, so the bound must be enforced explicitly or a
    # huge value silently "coerces" and the in-memory harness diverges from the
    # wire/HTTP path (which uses the real server).
    from par_rt_db import Cast
    from par_rt_db.in_memory import _coerce_value

    too_big = 2**63  # one past i64 max
    too_small = -(2**63) - 1  # one below i64 min
    # In-range int/float/str coerce to their decimal-string wire form ...
    assert _coerce_value(Cast.TO_INT64, 0) == "0"
    assert _coerce_value(Cast.TO_INT64, 2**63 - 1) == str(2**63 - 1)
    assert _coerce_value(Cast.TO_INT64, -(2**63)) == str(-(2**63))
    assert _coerce_value(Cast.TO_INT64, 42.0) == "42"
    assert _coerce_value(Cast.TO_INT64, "42") == "42"
    # ... out-of-range int / float / str all reject (parity with the server).
    assert _coerce_value(Cast.TO_INT64, too_big) is None
    assert _coerce_value(Cast.TO_INT64, too_small) is None
    assert _coerce_value(Cast.TO_INT64, float(too_big)) is None
    assert _coerce_value(Cast.TO_INT64, str(too_big)) is None
    # Non-integer floats still reject (unchanged behavior).
    assert _coerce_value(Cast.TO_INT64, 1.5) is None


def test_migrate_drop_table_clears_rows_and_schema() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    directives = Migration.builder().drop_table("users").build().directives
    result = c.migrate_schema(directives)

    assert result.applied is True
    assert result.directives[0].op == "dropTable"
    assert result.directives[0].affected_rows == 2
    assert "users" not in result.schema_.tables
    assert c.collect_all("users") == []


def test_migrate_eval_expr_raises_bad_request() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    directives = (
        Migration.builder().eval_expr("users", "upper", "upper(doc->>'name')").build().directives
    )
    with pytest.raises(RtDbError) as ei:
        c.migrate_schema(directives)
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "evalExpr unsupported" in ei.value.message


def test_migrate_dry_run_leaves_state_unchanged() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    original_docs = c.collect_all("users")
    directives = (
        Migration.builder().rename_field("users", "name", "fullName").dry_run().build().directives
    )
    result = c.migrate_schema(directives, dry_run=True)

    assert result.applied is False
    # Derived schema shows the rename.
    assert "fullName" in result.schema_.tables["users"].fields
    # But live state is unchanged.
    assert c.collect_all("users") == original_docs
    live_schema = c.to_schema_json()
    assert live_schema is not None
    assert "name" in live_schema.tables["users"].fields
    assert "fullName" not in live_schema.tables["users"].fields


def test_migrate_rejects_missing_source_field() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    directives = Migration.builder().rename_field("users", "nonexistent", "x").build().directives
    with pytest.raises(RtDbError) as ei:
        c.migrate_schema(directives)
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "does not exist" in ei.value.message
