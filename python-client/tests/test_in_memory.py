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
    MAX_AFFECTED_ROWS_PER_TXN,
    MAX_BY_QUERY_STEPS_PER_TXN,
    MAX_STEPS,
    InMemoryRtDbClient,
    InMemoryRtDbClientOptions,
    is_hex_id,
    worst_case_affected,
)
from par_rt_db.query import Query
from par_rt_db.schema import Schema, t
from par_rt_db.wire import AggregateSpec, ScheduleWhen, StepRetry, WorkflowSpec, WorkflowStepSpec

_when = TypeAdapter(ScheduleWhen)


def _inserted(res: StepResult) -> bool:
    """``True`` iff ``res`` is an upsert result with ``inserted=True``."""
    assert res is not None
    return bool(res.model_dump().get("inserted"))


def _id_of(res: StepResult) -> str:
    """Extract the ``id`` of an insert/upsert ``StepResult`` via ``model_dump``
    (the union also carries ``patchByQuery``/``deleteByQuery`` shapes with no
    ``id``, so attribute access does not type-check)."""
    assert res is not None
    return str(res.model_dump()["id"])


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
                # Not in the Rust/TS `items` fixtures — the search-terminal
                # tests need a real search surface (the shared prologue rejects
                # btree names for both modes, as the server does).
                .search_index("search_name", ["name"])
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
                .search_index("search_name", ["name"])
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
    assert is_hex_id(_id_of(res))
    doc = c.run_query(TableQuery("items").get(_id_of(res)).build())
    assert doc["name"] == "a"
    assert doc["status"] == "todo"
    assert doc["order"] == 1
    assert doc["_id"] == _id_of(res)
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
    doc = c.run_query(TableQuery("items").get(_id_of(res)).build())
    assert "note" not in doc  # null optional is stored as "key absent"


def test_insert_rejects_missing_required_field() -> None:
    c = _new_client()
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().insert("items", {"name": "a"}).build())
    assert ei.value.code is ErrorCode.SCHEMA_VIOLATION
    assert "status" in ei.value.message  # required field reported


# ---------------------------------------------------------------------------
# unique-index enforcement (CONFLICT) — mirrors server CREATE UNIQUE INDEX
# ---------------------------------------------------------------------------


def _unique_schema() -> Any:
    """A ``users`` table with a unique btree index on ``email``."""
    return (
        Schema.builder()
        .table(
            "users",
            lambda tb: (
                tb.field("email", t.string())
                .field("status", t.string())
                .index("by_email", ["email"])
                .unique()
            ),
        )
        .build()
    )


def _partial_unique_schema() -> Any:
    """A ``users`` table with a partial unique index on ``email`` whose predicate
    is ``status == "active"`` — only active rows are constrained."""
    from pydantic import TypeAdapter

    from par_rt_db.wire import FilterExpr

    pred = TypeAdapter(FilterExpr).validate_python(
        {"op": "eq", "field": "status", "value": "active"}
    )
    return (
        Schema.builder()
        .table(
            "users",
            lambda tb: (
                tb.field("email", t.string())
                .field("status", t.string())
                .index("by_email", ["email"])
                .unique()
                .where(pred)
            ),
        )
        .build()
    )


def _unique_client(schema: Any) -> InMemoryRtDbClient:
    counter = [1_700_000_000_000]

    def now() -> int:
        v = counter[0]
        counter[0] += 1
        return v

    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.0))
    c.push_schema(schema)
    return c


def test_unique_index_insert_collision_raises_conflict() -> None:
    c = _unique_client(_unique_schema())
    c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "on"}).build())
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "off"}).build())
    assert ei.value.code is ErrorCode.CONFLICT
    assert "unique index 'by_email'" in ei.value.message
    # The rolled-back insert left only the first row.
    docs = c.collect_all("users")
    assert len(docs) == 1
    assert docs[0]["status"] == "on"


def test_unique_index_insert_collision_rolls_back_whole_txn() -> None:
    c = _unique_client(_unique_schema())
    c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "on"}).build())
    # A second step in the same txn must also roll back when the later step
    # collides — atomicity mirrors the PRECONDITION_FAILED rollback path.
    with pytest.raises(RtDbError):
        c.mutate(
            Mutation.builder()
            .insert("users", {"email": "b@x", "status": "on"})
            .insert("users", {"email": "a@x", "status": "off"})
            .build()
        )
    docs = c.collect_all("users")
    assert {d["email"] for d in docs} == {"a@x"}  # b@x rolled back too


def test_unique_index_patch_collision_raises_conflict() -> None:
    c = _unique_client(_unique_schema())
    [r1] = c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "on"}).build())
    [r2] = c.mutate(Mutation.builder().insert("users", {"email": "b@x", "status": "on"}).build())
    assert r1 is not None and r2 is not None
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().patch("users", _id_of(r2), {"email": "a@x"}).build())
    assert ei.value.code is ErrorCode.CONFLICT
    # r2 kept its original email (write rolled back).
    patched = c.get("users", _id_of(r2))
    assert patched is not None and patched["email"] == "b@x"


def test_unique_index_replace_collision_raises_conflict() -> None:
    c = _unique_client(_unique_schema())
    [r1] = c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "on"}).build())
    [r2] = c.mutate(Mutation.builder().insert("users", {"email": "b@x", "status": "on"}).build())
    assert r1 is not None and r2 is not None
    with pytest.raises(RtDbError) as ei:
        c.mutate(
            Mutation.builder()
            .replace("users", _id_of(r2), {"email": "a@x", "status": "off"})
            .build()
        )
    assert ei.value.code is ErrorCode.CONFLICT
    replaced = c.get("users", _id_of(r2))
    assert replaced is not None and replaced["email"] == "b@x"


def test_unique_index_upsert_insert_collision_raises_conflict() -> None:
    """Upsert's INSERT path (eq lookup misses) still runs the unique check: an
    insert doc whose ``email`` duplicates an existing row raises ``CONFLICT``.
    The lookup index (``by_status``) is distinct from the unique index
    (``by_email``) — an upsert on a unique index can never collide on its own
    insert path (its eq lookup is authoritative for that key)."""
    from par_rt_db.schema import Schema, t

    schema = (
        Schema.builder()
        .table(
            "users",
            lambda tb: (
                tb.field("email", t.string())
                .field("status", t.string())
                .index("by_email", ["email"])
                .unique()
                .index("by_status", ["status"])
            ),
        )
        .build()
    )
    c = _unique_client(schema)
    c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "on"}).build())
    with pytest.raises(RtDbError) as ei:
        c.mutate(
            Mutation.builder()
            .upsert(
                "users",
                "by_status",
                ["missing"],
                insert={"email": "a@x", "status": "archived"},
                patch={"status": "archived"},
            )
            .build()
        )
    assert ei.value.code is ErrorCode.CONFLICT
    # The rolled-back insert left only the original row.
    docs = c.collect_all("users")
    assert len(docs) == 1
    assert docs[0]["status"] == "on"


def test_unique_index_null_key_is_distinct() -> None:
    """A null/absent key field disables the constraint for that row (Postgres
    ``UNIQUE`` treats NULLs as distinct) — two rows with a missing ``email``
    may coexist."""
    from par_rt_db.schema import Schema, t

    schema = (
        Schema.builder()
        .table(
            "users",
            lambda tb: (
                tb.field("email", t.optional(t.string()))
                .field("status", t.string())
                .index("by_email", ["email"])
                .unique()
            ),
        )
        .build()
    )
    c = _unique_client(schema)
    c.mutate(Mutation.builder().insert("users", {"email": None, "status": "on"}).build())
    c.mutate(Mutation.builder().insert("users", {"email": None, "status": "off"}).build())
    docs = c.collect_all("users")
    assert len(docs) == 2  # both null-key rows coexist


def test_partial_unique_index_allows_excluded_duplicate() -> None:
    """A partial unique index whose predicate is ``status == "active"`` does NOT
    constrain rows with a different status — two inactive rows may share an
    email."""
    c = _unique_client(_partial_unique_schema())
    c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "inactive"}).build())
    c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "inactive"}).build())
    docs = c.collect_all("users")
    assert len(docs) == 2  # predicate-excluded duplicates are allowed


def test_partial_unique_index_matching_duplicate_raises_conflict() -> None:
    """The same partial index DOES constrain active rows — a second active row
    sharing the email raises ``CONFLICT``."""
    c = _unique_client(_partial_unique_schema())
    c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "active"}).build())
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "active"}).build())
    assert ei.value.code is ErrorCode.CONFLICT
    assert "unique index 'by_email'" in ei.value.message


def test_partial_unique_index_candidate_excluded_skips_check() -> None:
    """When the CANDIDATE row does not match the predicate, the constraint is
    skipped for it — an inactive candidate may be inserted even if an active row
    owns the same email (the candidate is outside the constrained set)."""
    c = _unique_client(_partial_unique_schema())
    c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "active"}).build())
    c.mutate(Mutation.builder().insert("users", {"email": "a@x", "status": "archived"}).build())
    docs = c.collect_all("users")
    assert {d["status"] for d in docs} == {"active", "archived"}


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
        ids.append(_id_of(res))
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


# --- filter: typed int64 comparison (ENH-027 parity fix) ---------------------
#
# Found by ``server/tests/proptest_parity.rs``: a string filter value on a
# declared ``int64`` field is the server's typed-``bigint`` wire form and must
# order NUMERICALLY. The pre-fix evaluator compared the decimal strings
# lexicographically ("15" < "9"), diverging from Postgres on every
# digit-count-variant pair (the proptest counterexample was ``not(gt "-1")``
# dropping a row with ``-605``, which the server keeps).


def _int64_typed_fields() -> dict[str, Any]:
    """Declared field map for the typed int64 comparison tests: the parity
    fix against the server's typed bigint column path."""
    return {"n": t.int64(), "opt_n": t.optional(t.int64()), "name": t.string()}


def test_int64_string_filter_values_order_numerically() -> None:
    from par_rt_db.in_memory import _eval_filter_expr

    fields = _int64_typed_fields()
    # The minimized proptest counterexample: "-605" > "-1" lexicographically
    # ('6' sorts after '1') but -605 > -1 is false numerically — the server
    # keeps this row, the pre-fix engine dropped it.
    assert (
        _eval_filter_expr(_flt({"op": "gt", "field": "n", "value": "-1"}), {"n": "-605"}, fields)
        is False
    )
    # 15 > 9 numerically; lexicographically "15" < "9".
    assert (
        _eval_filter_expr(_flt({"op": "gt", "field": "n", "value": "9"}), {"n": "15"}, fields)
        is True
    )
    assert (
        _eval_filter_expr(_flt({"op": "lt", "field": "n", "value": "100"}), {"n": "99"}, fields)
        is True
    )
    # Boundaries compare exactly (i64::MAX is not float-exact).
    assert (
        _eval_filter_expr(
            _flt({"op": "lte", "field": "n", "value": "9223372036854775807"}),
            {"n": "9223372036854775806"},
            fields,
        )
        is True
    )
    assert (
        _eval_filter_expr(
            _flt({"op": "gte", "field": "n", "value": "-9223372036854775808"}),
            {"n": "-9223372036854775807"},
            fields,
        )
        is True
    )
    assert (
        _eval_filter_expr(
            _flt({"op": "gte", "field": "n", "value": "9223372036854775807"}),
            {"n": "9223372036854775806"},
            fields,
        )
        is False
    )
    # eq on canonical decimal strings still matches exactly.
    assert (
        _eval_filter_expr(_flt({"op": "eq", "field": "n", "value": "42"}), {"n": "42"}, fields)
        is True
    )


def test_int64_typed_comparison_unwraps_optional_and_keeps_null_exclusion() -> None:
    from par_rt_db.in_memory import _eval_filter_expr

    fields = _int64_typed_fields()
    # optional<int64> unwraps to the same typed comparison.
    assert (
        _eval_filter_expr(
            _flt({"op": "gt", "field": "opt_n", "value": "9"}), {"opt_n": "15"}, fields
        )
        is True
    )
    # null/absent doc fields never match (SQL NULL exclusion) — unchanged.
    assert (
        _eval_filter_expr(
            _flt({"op": "gt", "field": "opt_n", "value": "9"}), {"other": "15"}, fields
        )
        is False
    )
    assert (
        _eval_filter_expr(
            _flt({"op": "gt", "field": "opt_n", "value": "9"}), {"opt_n": None}, fields
        )
        is False
    )
    # `in` goes through the same leaf path, so it inherits the fix.
    assert (
        _eval_filter_expr(
            _flt({"op": "in", "field": "n", "values": ["9", "15"]}), {"n": "15"}, fields
        )
        is True
    )


def test_int64_number_filter_values_still_compare_as_float8() -> None:
    # The jsonb-path wire form (non-indexed int64 field): a NUMBER value
    # compares via `(doc->>'f')::float8` on the server — unchanged behavior.
    from par_rt_db.in_memory import _eval_filter_expr

    fields = _int64_typed_fields()
    assert (
        _eval_filter_expr(_flt({"op": "gt", "field": "n", "value": 0}), {"n": "15"}, fields) is True
    )
    assert (
        _eval_filter_expr(_flt({"op": "lt", "field": "n", "value": 9}), {"n": "15"}, fields)
        is False
    )


def test_search_filter_narrows_the_matched_set() -> None:
    # The tsquery approximation matches words for real (FM-31); a declared
    # terminal filter narrows the matched set via the same FilterExpr
    # evaluator the db-side .filter() uses.
    from par_rt_db.wire import FilterExpr

    c = _new_search_client()
    for row in (
        {"title": "release notes", "body": "x", "status": "open"},
        {"title": "meeting notes", "body": "x", "status": "open"},
        {"title": "cooking notes", "body": "x", "status": "closed"},
    ):
        c.mutate(Mutation.builder().insert("notes", row).build())
    flt = TypeAdapter(FilterExpr).validate_python({"op": "eq", "field": "status", "value": "open"})
    docs = c.run_query(TableQuery("notes").search("search_all", "notes", filter_=flt).build())
    assert {d["title"] for d in docs} == {"release notes", "meeting notes"}
    # Without a filter every word-match is a hit.
    all_docs = c.run_query(TableQuery("notes").search("search_all", "notes").build())
    assert {d["title"] for d in all_docs} == {"release notes", "meeting notes", "cooking notes"}
    # A compound FilterExpr narrows further.
    flt2 = TypeAdapter(FilterExpr).validate_python(
        {
            "op": "and",
            "exprs": [
                {"op": "eq", "field": "status", "value": "open"},
                {"op": "eq", "field": "title", "value": "release notes"},
            ],
        }
    )
    docs2 = c.run_query(TableQuery("notes").search("search_all", "notes", filter_=flt2).build())
    assert {d["title"] for d in docs2} == {"release notes"}


def test_search_filter_rejects_unknown_field() -> None:
    # The nested search filter is structurally validated against the table's
    # declared fields, mirroring the server's compile_filter BadRequest.
    from par_rt_db.wire import FilterExpr

    c = _new_client()
    _seed_query_rows(c)
    flt = TypeAdapter(FilterExpr).validate_python({"op": "eq", "field": "nope", "value": 1})
    with pytest.raises(RtDbError) as ei:
        c.run_query(TableQuery("items").search("search_name", "x", filter_=flt).build())
    assert ei.value.code is ErrorCode.BAD_REQUEST


# --- search mode="trgm" (FM-30): substring matching + similarity ranking -----


def _new_search_client() -> InMemoryRtDbClient:
    # Same post-incrementing clock as `_new_client`, so `created_at` ascends
    # with insertion order (the trgm ranking tie-break is created_at desc).
    counter = [1_700_000_000_000]

    def now() -> int:
        v = counter[0]
        counter[0] += 1
        return v

    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.0))
    c.push_schema(
        Schema.builder()
        .table(
            "notes",
            lambda tb: (
                tb.field("title", t.string())
                .field("body", t.string())
                .field("status", t.string())
                .search_index("search_all", ["title", "body"])
                .index("by_status", ["status"])
            ),
        )
        .build()
    )
    return c


def _seed_trgm_rows(c: InMemoryRtDbClient) -> None:
    # Four notes; trgm scores for query "conv" (len 4) annotated per row —
    # score = 4 / len(matching field), max across the index's two fields:
    #   "convex"               -> 4/6  (title, the closest match)
    #   "ConVex Mirror"        -> 4/13 (title, case-insensitive hit)
    #   "convexity explained"  -> 4/19 (body)
    #   "totally unrelated"    -> no match
    for row in (
        {"title": "convex", "body": "intro to shapes", "status": "open"},
        {"title": "misc", "body": "convexity explained", "status": "open"},
        {"title": "other", "body": "totally unrelated", "status": "closed"},
        {"title": "ConVex Mirror", "body": "words", "status": "closed"},
    ):
        c.mutate(Mutation.builder().insert("notes", row).build())


def test_search_trgm_matches_substrings() -> None:
    # "conv" is an infix of "convex"/"convexity explained" — a plainto_tsquery
    # lexeme would match none of these (FM-30's motivating case).
    c = _new_search_client()
    _seed_trgm_rows(c)
    docs = c.run_query(TableQuery("notes").search("search_all", "conv", mode="trgm").build())
    assert {d["title"] for d in docs} == {"convex", "misc", "ConVex Mirror"}
    assert all(d["title"] != "other" for d in docs)


def test_search_trgm_is_case_insensitive() -> None:
    # Doc-side folding ("conv" matches the mixed-case title "ConVex Mirror")
    # and query-side folding ("CONV" matches the lowercase title "convex").
    c = _new_search_client()
    _seed_trgm_rows(c)
    docs = c.run_query(TableQuery("notes").search("search_all", "CONV", mode="trgm").build())
    assert {d["title"] for d in docs} == {"convex", "misc", "ConVex Mirror"}


def test_search_trgm_ranks_by_similarity() -> None:
    # Shorter containing field = closer match: 4/6 > 4/13 > 4/19.
    c = _new_search_client()
    _seed_trgm_rows(c)
    docs = c.run_query(TableQuery("notes").search("search_all", "conv", mode="trgm").build())
    assert [d["title"] for d in docs] == ["convex", "ConVex Mirror", "misc"]


def test_search_trgm_rank_takes_max_across_fields() -> None:
    # The doc's score is its BEST matching field, not the first: "convex"/"conv"
    # scores max(4/6, 4/4) = 1.0 via body, beating "convx" (4/5) — a
    # first-field-only score (4/6) would rank it below.
    c = _new_search_client()
    for row in (
        {"title": "convex", "body": "conv", "status": "open"},
        {"title": "zz", "body": "convx", "status": "open"},
    ):
        c.mutate(Mutation.builder().insert("notes", row).build())
    docs = c.run_query(TableQuery("notes").search("search_all", "conv", mode="trgm").build())
    assert [d["title"] for d in docs] == ["convex", "zz"]


def test_search_trgm_tiebreaks_created_at_desc() -> None:
    # Equal scores (both bodies "convex twin", 4/11) tie-break created_at desc:
    # the LATER insert ranks first.
    c = _new_search_client()
    for row in (
        {"title": "older", "body": "convex twin", "status": "open"},
        {"title": "newer", "body": "convex twin", "status": "open"},
    ):
        c.mutate(Mutation.builder().insert("notes", row).build())
    docs = c.run_query(TableQuery("notes").search("search_all", "conv", mode="trgm").build())
    assert [d["title"] for d in docs] == ["newer", "older"]


def test_search_trgm_composes_with_filter_and_take() -> None:
    from par_rt_db.wire import FilterExpr

    c = _new_search_client()
    _seed_trgm_rows(c)
    flt = TypeAdapter(FilterExpr).validate_python({"op": "eq", "field": "status", "value": "open"})
    docs = c.run_query(
        TableQuery("notes").search("search_all", "conv", mode="trgm", filter_=flt).build()
    )
    # Only the two "open" notes; still similarity-ranked.
    assert [d["title"] for d in docs] == ["convex", "misc"]
    # take(1) keeps the top-ranked of the filtered set.
    top = c.run_query(
        TableQuery("notes").search("search_all", "conv", mode="trgm", filter_=flt).take(1).build()
    )
    assert [d["title"] for d in top] == ["convex"]


def test_search_trgm_requires_a_search_index() -> None:
    # trgm looks up the named index for its field list; a btree index (or a
    # missing name) is not a search surface -> BAD_REQUEST, mirroring the
    # server's index resolution.
    c = _new_search_client()
    _seed_trgm_rows(c)
    with pytest.raises(RtDbError) as ei:
        c.run_query(TableQuery("notes").search("by_status", "conv", mode="trgm").build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "search index 'by_status' not found" in ei.value.message


def test_search_tsquery_requires_a_search_index() -> None:
    # The index check lives in the shared prologue (server compile_search runs
    # it before the mode branch), so the tsquery stub rejects a btree name too.
    c = _new_search_client()
    _seed_trgm_rows(c)
    with pytest.raises(RtDbError) as ei:
        c.run_query(TableQuery("notes").search("by_status", "conv").build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "search index 'by_status' not found" in ei.value.message


def test_search_rejects_empty_query_in_both_modes() -> None:
    # Empty (or whitespace-only) query text is BAD_REQUEST before the mode
    # branch, mirroring the server's compile_search and the ts harness.
    from par_rt_db.wire import SearchMode

    c = _new_search_client()
    _seed_trgm_rows(c)
    modes: tuple[SearchMode | None, ...] = (None, "tsquery", "trgm")
    for mode in modes:
        for query in ("", "   "):
            with pytest.raises(RtDbError) as ei:
                c.run_query(TableQuery("notes").search("search_all", query, mode=mode).build())
            assert ei.value.code is ErrorCode.BAD_REQUEST
            assert ei.value.message == "search query text must not be empty"


def test_search_explicit_tsquery_mode_behaves_like_default() -> None:
    # mode="tsquery" is the default: the websearch approximation runs
    # unchanged. The word "convex" matches the docs CARRYING that word — not
    # "convexity explained" (no stemming) and not "totally unrelated" — and
    # case-insensitively catches "ConVex Mirror".
    c = _new_search_client()
    _seed_trgm_rows(c)
    default = c.run_query(TableQuery("notes").search("search_all", "convex").build())
    explicit = c.run_query(
        TableQuery("notes").search("search_all", "convex", mode="tsquery").build()
    )
    assert explicit == default
    assert {d["title"] for d in explicit} == {"convex", "ConVex Mirror"}


# --- phrase/operator search + snippets (FM-31) --------------------------------
#
# Mirrors the server's `server/tests/search_test.rs` FM-31 block: the tsquery
# approximation models websearch_to_tsquery's operator semantics (phrase
# adjacency, `or` union, `-term` exclusion) and the `_searchSnippet`
# ts_headline stand-in.


def _seed_note(c: InMemoryRtDbClient, **fields: Any) -> None:
    """Insert one ``notes`` row (the FM-31 fixtures' seeding shorthand)."""
    c.mutate(Mutation.builder().insert("notes", fields).build())


def test_search_phrase_query_requires_adjacent_words() -> None:
    # A quoted phrase requires the words ADJACENT (case-insensitive): only the
    # doc where "database notes" appears contiguously matches. The same words
    # unquoted still match both — AND semantics, pinning plain-query
    # equivalence with the former plainto_tsquery behavior.
    c = _new_search_client()
    _seed_note(c, title="adjacent", body="the database notes are great", status="open")
    _seed_note(c, title="apart", body="notes about the database", status="open")

    docs = c.run_query(TableQuery("notes").search("search_all", '"database notes"').build())
    assert [d["title"] for d in docs] == ["adjacent"]

    docs = c.run_query(TableQuery("notes").search("search_all", "database notes").build())
    assert {d["title"] for d in docs} == {"adjacent", "apart"}


def test_search_or_operator_unions_alternatives() -> None:
    # The bare word `or` unions alternatives: a doc with either term matches,
    # one with neither does not.
    c = _new_search_client()
    for title in ("alpha only", "beta only", "gamma"):
        _seed_note(c, title=title, body="filler", status="open")

    docs = c.run_query(TableQuery("notes").search("search_all", "alpha or beta").build())
    assert {d["title"] for d in docs} == {"alpha only", "beta only"}
    assert all(d["title"] != "gamma" for d in docs)


def test_search_minus_operator_excludes_term() -> None:
    # `-term` excludes docs carrying the negated word while keeping the
    # positive one.
    c = _new_search_client()
    _seed_note(c, title="database intro", body="database basics", status="open")
    _seed_note(c, title="database cooking", body="database recipes", status="open")

    docs = c.run_query(TableQuery("notes").search("search_all", "database -cooking").build())
    assert [d["title"] for d in docs] == ["database intro"]


def test_search_snippet_returns_highlighted_fragment() -> None:
    # snippet=True attaches a `_searchSnippet` to every hit — an excerpt with
    # the matched term wrapped in <mark>, honoring the 35-word cap. Omitted,
    # no snippet field appears.
    c = _new_search_client()
    _seed_note(c, title="database notes", body="a database is a database", status="open")

    docs = c.run_query(TableQuery("notes").search("search_all", "database", snippet=True).build())
    assert len(docs) == 1
    snippet = docs[0]["_searchSnippet"]
    assert isinstance(snippet, str)
    assert "<mark>database</mark>" in snippet
    assert len(snippet.split()) <= 35

    plain = c.run_query(TableQuery("notes").search("search_all", "database").build())
    assert len(plain) == 1
    assert "_searchSnippet" not in plain[0]


def test_search_snippet_caps_at_35_words() -> None:
    # A long match deep in the text still yields a bounded excerpt that
    # contains the marked term (the MaxWords=35 stand-in).
    c = _new_search_client()
    body = " ".join(f"w{i}" for i in range(100)) + " needle "
    body += " ".join(f"z{i}" for i in range(50))
    _seed_note(c, title="long", body=body, status="open")

    docs = c.run_query(TableQuery("notes").search("search_all", "needle", snippet=True).build())
    snippet = docs[0]["_searchSnippet"]
    assert len(snippet.split()) == 35
    assert snippet.startswith("<mark>needle</mark>")


def test_search_snippet_false_behaves_like_omitted() -> None:
    # An explicit snippet=False behaves exactly like omission (the wire
    # defaults to off; nothing is rendered).
    c = _new_search_client()
    _seed_note(c, title="database notes", body="", status="open")

    docs = c.run_query(TableQuery("notes").search("search_all", "database", snippet=False).build())
    assert len(docs) == 1
    assert "_searchSnippet" not in docs[0]


def test_search_snippet_highlights_phrase_queries() -> None:
    # A phrase hit renders as adjacent per-word marks, like ts_headline over
    # the same websearch_to_tsquery the WHERE matched.
    c = _new_search_client()
    _seed_note(c, title="adjacent", body="the database notes are great today", status="open")

    docs = c.run_query(
        TableQuery("notes").search("search_all", '"database notes"', snippet=True).build()
    )
    assert len(docs) == 1
    snippet = docs[0]["_searchSnippet"]
    assert "<mark>database</mark> <mark>notes</mark>" in snippet


def test_search_snippet_composes_with_filter() -> None:
    # Snippets compose with `filter`: narrowed hits are snippeted,
    # filtered-out rows are neither returned nor snippeted.
    from par_rt_db.wire import FilterExpr

    c = _new_search_client()
    for title, status in (
        ("database intro", "open"),
        ("database advanced", "closed"),
        ("cooking", "open"),
    ):
        _seed_note(c, title=title, body="", status=status)
    flt = TypeAdapter(FilterExpr).validate_python({"op": "eq", "field": "status", "value": "open"})

    docs = c.run_query(
        TableQuery("notes").search("search_all", "database", snippet=True, filter_=flt).build()
    )
    assert [d["title"] for d in docs] == ["database intro"]
    assert "<mark>database</mark>" in docs[0]["_searchSnippet"]


def test_search_snippet_rejected_with_trgm_mode() -> None:
    # snippet + trgm is rejected up front — trgm matches substrings, so there
    # is no tsquery tree to highlight (mirrors the server's compile_search).
    c = _new_search_client()
    _seed_trgm_rows(c)
    with pytest.raises(RtDbError) as ei:
        c.run_query(
            TableQuery("notes").search("search_all", "conv", mode="trgm", snippet=True).build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert ei.value.message == "snippet is only supported in tsquery mode"


def test_vector_search_filter_narrows_the_candidate_set() -> None:
    # The in-memory vectorSearch stub does not rank by vector similarity (every
    # table row is a candidate — the sound over-approximation); a declared
    # terminal filter narrows that set via the same FilterExpr evaluator the
    # db-side .filter() and search use.
    from par_rt_db.wire import FilterExpr

    c = _new_client()
    _seed_query_rows(c)  # a/todo/2, b/todo/1, c/done/3
    flt = TypeAdapter(FilterExpr).validate_python({"op": "eq", "field": "status", "value": "todo"})
    docs = c.run_query(
        TableQuery("items").vector_search("vec", [1.0, 0.0], limit=5, filter_=flt).build()
    )
    assert {d["name"] for d in docs} == {"a", "b"}
    # Without a filter every table row is a candidate.
    all_docs = c.run_query(TableQuery("items").vector_search("vec", [1.0, 0.0], limit=5).build())
    assert {d["name"] for d in all_docs} == {"a", "b", "c"}
    # A compound FilterExpr narrows further.
    flt2 = TypeAdapter(FilterExpr).validate_python(
        {
            "op": "and",
            "exprs": [
                {"op": "eq", "field": "status", "value": "todo"},
                {"op": "gt", "field": "order", "value": 1},
            ],
        }
    )
    docs2 = c.run_query(
        TableQuery("items").vector_search("vec", [1.0, 0.0], limit=5, filter_=flt2).build()
    )
    assert {d["name"] for d in docs2} == {"a"}


def test_vector_search_filter_rejects_unknown_field() -> None:
    # The nested vectorSearch filter is structurally validated against the
    # table's declared fields, mirroring search and the server's compile_filter.
    from par_rt_db.wire import FilterExpr

    c = _new_client()
    _seed_query_rows(c)
    flt = TypeAdapter(FilterExpr).validate_python({"op": "eq", "field": "nope", "value": 1})
    with pytest.raises(RtDbError) as ei:
        c.run_query(
            TableQuery("items").vector_search("vec", [1.0, 0.0], limit=5, filter_=flt).build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST


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


def test_aggregate_count_scalar_returns_row_count() -> None:
    # `count` aggregates rows (COUNT(*)) and consumes no aggregate field; a
    # scalar count needs no index field beyond the eq prefix.
    c = _new_client()
    _seed_orders(c, [3, 1, 2])
    assert (
        c.run_query(
            TableQuery("items")
            .with_index("by_status_and_order")
            .eq("todo")
            .aggregate("count")
            .build()
        )
        == 3
    )


def test_aggregate_count_scalar_over_empty_set_is_zero() -> None:
    # Unlike the field-bearing ops (which yield None over an empty set), count
    # returns 0.
    c = _new_client()
    _seed_orders(c, [1])
    assert (
        c.run_query(
            TableQuery("items")
            .with_index("by_status_and_order")
            .eq("missing")
            .aggregate("count")
            .build()
        )
        == 0
    )


def test_aggregate_count_scalar_needs_no_index() -> None:
    # A scalar count with no index counts every row in the table.
    c = _new_client()
    _seed_orders(c, [1, 2])
    _seed_orders(c, [3], status="done")
    assert c.run_query(TableQuery("items").aggregate("count").build()) == 3


def test_aggregate_count_grouped_returns_count_per_group() -> None:
    # A grouped count needs one index field beyond the eq prefix to group by;
    # it returns the row count per group.
    c = _new_client()
    for status, order in [("todo", 1), ("todo", 2), ("done", 3), ("done", 4), ("done", 5)]:
        c.mutate(
            Mutation.builder()
            .insert("items", {"name": "n", "status": status, "order": order})
            .build()
        )
    v = c.run_query(
        TableQuery("items")
        .with_index("by_status_and_order")
        .aggregate("count", group_by=True)
        .build()
    )
    assert v == [{"key": "done", "value": 3}, {"key": "todo", "value": 2}]


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
    c.mutate(Mutation.builder().patch("items", _id_of(res), {"status": "done"}).build())
    doc = c.run_query(TableQuery("items").get(_id_of(res)).build())
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
        .replace("items", _id_of(res), {"name": "a2", "status": "done", "order": 9})
        .build()
    )
    doc = c.run_query(TableQuery("items").get(_id_of(res)).build())
    assert doc["name"] == "a2"
    assert doc["order"] == 9
    assert doc["_version"] == 2


def test_delete_removes_the_doc() -> None:
    c = _new_client()
    [res] = c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    )
    assert res is not None
    c.mutate(Mutation.builder().delete("items", _id_of(res)).build())
    assert c.run_query(TableQuery("items").get(_id_of(res)).build()) is None


def test_delete_unknown_id_is_not_found() -> None:
    c = _new_client()
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().delete("items", "deadbeef").build())
    assert ei.value.code is ErrorCode.NOT_FOUND


def _flt(expr: dict[str, object]) -> Any:
    from par_rt_db.wire import FilterExpr

    return TypeAdapter(FilterExpr).validate_python(expr)


def test_patch_by_query_patches_matching_rows_and_reports_count() -> None:
    c = _new_client()
    for status in ("todo", "todo", "done"):
        c.mutate(
            Mutation.builder().insert("items", {"name": "n", "status": status, "order": 1}).build()
        )
    flt = _flt({"op": "eq", "field": "status", "value": "todo"})
    [res] = c.mutate(Mutation.builder().patch_by_query("items", flt, {"status": "done"}).build())
    assert res is not None
    assert res.model_dump(by_alias=True, mode="json") == {"patched": 2, "truncated": False}
    # All rows are now done.
    docs = c.run_query(TableQuery("items").build())
    assert all(d["status"] == "done" for d in docs)
    # The two patched rows bumped to version 2; the already-done row stays at 1.
    assert sorted(d["_version"] for d in docs) == [1, 2, 2]


def test_patch_by_query_truncates_at_limit() -> None:
    c = _new_client()
    for _ in range(3):
        c.mutate(
            Mutation.builder().insert("items", {"name": "n", "status": "todo", "order": 1}).build()
        )
    flt = _flt({"op": "eq", "field": "status", "value": "todo"})
    [res] = c.mutate(Mutation.builder().patch_by_query("items", flt, {"order": 9}, limit=2).build())
    assert res is not None
    dumped = res.model_dump(by_alias=True, mode="json")
    assert dumped["patched"] == 2
    assert dumped["truncated"] is True
    # Exactly 2 patched (order 9) + 1 untouched (order 1).
    docs = c.run_query(TableQuery("items").build())
    assert sum(1 for d in docs if d["order"] == 9) == 2


def test_patch_by_query_no_matches_reports_zero() -> None:
    c = _new_client()
    c.mutate(
        Mutation.builder().insert("items", {"name": "n", "status": "todo", "order": 1}).build()
    )
    flt = _flt({"op": "eq", "field": "status", "value": "missing"})
    [res] = c.mutate(Mutation.builder().patch_by_query("items", flt, {"order": 9}).build())
    assert res is not None
    assert res.model_dump(by_alias=True, mode="json") == {"patched": 0, "truncated": False}


def test_delete_by_query_removes_matching_rows_and_reports_count() -> None:
    c = _new_client()
    for status in ("todo", "todo", "done"):
        c.mutate(
            Mutation.builder().insert("items", {"name": "n", "status": status, "order": 1}).build()
        )
    flt = _flt({"op": "eq", "field": "status", "value": "todo"})
    [res] = c.mutate(Mutation.builder().delete_by_query("items", flt).build())
    assert res is not None
    assert res.model_dump(by_alias=True, mode="json") == {"deleted": 2, "truncated": False}
    docs = c.run_query(TableQuery("items").build())
    assert len(docs) == 1
    assert docs[0]["status"] == "done"


def test_delete_by_query_truncates_at_limit() -> None:
    c = _new_client()
    for _ in range(3):
        c.mutate(
            Mutation.builder().insert("items", {"name": "n", "status": "todo", "order": 1}).build()
        )
    flt = _flt({"op": "eq", "field": "status", "value": "todo"})
    [res] = c.mutate(Mutation.builder().delete_by_query("items", flt, limit=2).build())
    assert res is not None
    dumped = res.model_dump(by_alias=True, mode="json")
    assert dumped["deleted"] == 2
    assert dumped["truncated"] is True
    assert len(c.run_query(TableQuery("items").build())) == 1


def test_sec104_rejects_over_budget_by_query_step_count() -> None:
    # Mirrors server `sec104_rejects_over_budget_by_query_step_count`. A txn
    # with MAX_BY_QUERY_STEPS_PER_TXN+1 patchByQuery steps is rejected at the
    # top of _execute_transaction, before any step applies. The original AUDIT
    # finding was 1024 by-query steps (~1M-row single-writer stall); the 16-step
    # cap rejects it pre-execution.
    assert MAX_BY_QUERY_STEPS_PER_TXN < 1024
    c = _new_client()
    c.mutate(
        Mutation.builder().insert("items", {"name": "seed", "status": "todo", "order": 0}).build()
    )
    flt = _flt({"op": "eq", "field": "status", "value": "todo"})
    b = Mutation.builder()
    for i in range(MAX_BY_QUERY_STEPS_PER_TXN + 1):
        b = b.patch_by_query("items", flt, {"order": i})
    with pytest.raises(RtDbError) as ei:
        c.mutate(b.build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "by-query steps" in ei.value.message
    # Pre-execution rejection commits nothing.
    docs = c.run_query(TableQuery("items").build())
    assert len(docs) == 1
    assert docs[0]["order"] == 0


def test_sec104_rejects_over_budget_aggregate_affected() -> None:
    # Mirrors server `sec104_rejects_over_budget_aggregate_affected`. A txn
    # with few by-query steps (under the step cap) but each at the default
    # 1000-row limit can still exceed MAX_AFFECTED_ROWS_PER_TXN; reject it.
    over_steps = (MAX_AFFECTED_ROWS_PER_TXN // 1000) + 1
    assert over_steps <= MAX_BY_QUERY_STEPS_PER_TXN
    c = _new_client()
    c.mutate(
        Mutation.builder().insert("items", {"name": "seed", "status": "todo", "order": 0}).build()
    )
    flt = _flt({"op": "eq", "field": "status", "value": "todo"})
    b = Mutation.builder()
    for _ in range(over_steps):
        b = b.delete_by_query("items", flt)
    with pytest.raises(RtDbError) as ei:
        c.mutate(b.build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "affect up to" in ei.value.message
    assert len(c.run_query(TableQuery("items").build())) == 1


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
    assert _id_of(r2) == _id_of(r1)
    doc = c.run_query(TableQuery("items").get(_id_of(r1)).build())
    assert doc["order"] == 5


def test_expect_version_passes_and_fails() -> None:
    c = _new_client()
    [res] = c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build()
    )
    assert res is not None
    # Correct version passes.
    c.mutate(Mutation.builder().expect_version("items", _id_of(res), 1).build())
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().expect_version("items", _id_of(res), 99).build())
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
    assert c.cancel_schedule(sid) is True
    assert c.list_schedules() == []
    # Second cancel is a no-op, not an error (server scheduler::cancel contract).
    assert c.cancel_schedule(sid) is False


# --- FM-28: schedule/cancelSchedule as transaction steps ----------------------


def test_schedule_step_enqueues_job_fired_by_tick() -> None:
    c, clock = _new_clock_client()
    inner = Mutation.builder().insert("items", {"name": "b", "status": "todo", "order": 2}).build()
    txn = (
        Mutation.builder()
        .insert("items", {"name": "a", "status": "todo", "order": 1})
        .schedule(_when.validate_python({"type": "afterMs", "ms": 1000}), inner)
        .build()
    )
    results = c.mutate(txn)
    # The insert applied now; the schedule step only enqueued (1 doc, 1 job).
    assert len(c.run_query(TableQuery("items").build())) == 1
    assert results[1] is not None
    schedule_id = str(results[1].model_dump(by_alias=True)["scheduleId"])
    jobs = c.list_schedules()
    assert len(jobs) == 1
    assert jobs[0].id == schedule_id
    # Advance past the due time: tick fires the nested txn.
    clock[0] += 2000
    c.tick()
    docs = c.run_query(TableQuery("items").build())
    assert sorted(d["name"] for d in docs) == ["a", "b"]
    # One-shot removed after its successful fire.
    assert c.list_schedules() == []


def test_cancel_schedule_step_removes_pending_job() -> None:
    c, clock = _new_clock_client()
    txn = (
        Mutation.builder()
        .schedule(_when.validate_python({"type": "afterMs", "ms": 1000}), _insert_todo_txn())
        .build()
    )
    scheduled = c.mutate(txn)[0]
    assert scheduled is not None
    schedule_id = str(scheduled.model_dump(by_alias=True)["scheduleId"])
    res = c.mutate(Mutation.builder().cancel_schedule(schedule_id).build())
    assert res[0] is not None
    assert res[0].model_dump(by_alias=True) == {"cancelled": True}
    # Past due, nothing fires — the job is gone.
    clock[0] += 2000
    c.tick()
    assert c.run_query(TableQuery("items").build()) == []
    # Cancelling a missing id is a False result, not an error (step semantics
    # differ deliberately from the standalone cancel op's NOT_FOUND).
    res2 = c.mutate(Mutation.builder().cancel_schedule(schedule_id).build())
    assert res2[0] is not None
    assert res2[0].model_dump(by_alias=True) == {"cancelled": False}


def test_recursive_step_budget_rejects_oversized_tree() -> None:
    c, _clock = _new_clock_client()
    # Each half fits the harness cap alone; only the recursive count (a
    # schedule step = 1 + its nested steps) trips it: half + 1 + half > MAX_STEPS.
    half = MAX_STEPS // 2
    nested = Mutation.builder()
    for i in range(half):
        nested.insert("items", {"name": f"n{i}", "status": "todo", "order": i})
    outer = Mutation.builder()
    for i in range(half):
        outer.insert("items", {"name": f"o{i}", "status": "todo", "order": i})
    outer.schedule(_when.validate_python({"type": "afterMs", "ms": 1000}), nested.build())
    with pytest.raises(RtDbError) as ei:
        c.mutate(outer.build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    # Rejected before any write: no docs, no enqueued job.
    assert c.run_query(TableQuery("items").build()) == []
    assert c.list_schedules() == []


def test_failed_txn_rolls_back_schedule_step_enqueue() -> None:
    # FM-28 rollback: the schedule step's enqueue joins the atomicity snapshot —
    # a later step's error must not leave a phantom job that tick() would fire
    # (mirrors the server's single sqlx transaction around the insert).
    c, clock = _new_clock_client()
    with pytest.raises(RtDbError) as ei:
        c.mutate(
            Mutation.builder()
            .schedule(
                _when.validate_python({"type": "afterMs", "ms": 1000}),
                _insert_todo_txn(),
            )
            .delete("items", "nonexistent")  # NOT_FOUND -> rollback the enqueue
            .build()
        )
    assert ei.value.code is ErrorCode.NOT_FOUND
    assert c.list_schedules() == []
    # Past the would-be due time: nothing fires.
    clock[0] += 2000
    c.tick()
    assert c.run_query(TableQuery("items").build()) == []


def test_failed_txn_rolls_back_cancel_schedule_step() -> None:
    # Same snapshot covers a cancel step's removal: a pre-existing job survives
    # a txn that cancelled it and then failed.
    c, clock = _new_clock_client()
    sid = c.schedule(_insert_todo_txn(), _when.validate_python({"type": "afterMs", "ms": 1000}))
    with pytest.raises(RtDbError):
        c.mutate(
            Mutation.builder()
            .cancel_schedule(sid)
            .delete("items", "nonexistent")  # NOT_FOUND -> rollback the cancel
            .build()
        )
    jobs = c.list_schedules()
    assert len(jobs) == 1 and jobs[0].id == sid
    # The surviving job still fires on its original schedule.
    clock[0] += 2000
    c.tick()
    assert len(c.run_query(TableQuery("items").build())) == 1


def test_nested_schedule_step_fires_via_tick() -> None:
    # schedule-in-schedule: the fired txn's own schedule step enqueues a new
    # job (server equivalent: txn.rs chained schedule_step test).
    c, clock = _new_clock_client()
    inner = Mutation.builder().insert("items", {"name": "b", "status": "todo", "order": 2}).build()
    outer_txn = (
        Mutation.builder()
        .insert("items", {"name": "a", "status": "todo", "order": 1})
        .schedule(_when.validate_python({"type": "afterMs", "ms": 1000}), inner)
        .build()
    )
    c.schedule(outer_txn, _when.validate_python({"type": "afterMs", "ms": 1000}))
    # Fire the outer job: its insert applies and its schedule step enqueues the
    # inner job (due at tick-time + 1000).
    clock[0] += 2000
    c.tick()
    assert [d["name"] for d in c.run_query(TableQuery("items").build())] == ["a"]
    assert len(c.list_schedules()) == 1
    # Fire the inner job: doc b lands, one-shot consumed.
    clock[0] += 2000
    c.tick()
    assert sorted(d["name"] for d in c.run_query(TableQuery("items").build())) == ["a", "b"]
    assert c.list_schedules() == []


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


def test_migrate_drop_field_counts_only_carriers() -> None:
    # `users.status` is optional; the _migrate_client fixture's two rows omit
    # it. Insert a third row that carries it, drop the field, and assert
    # affected_rows is the carrier count (1), not the total row count (3) —
    # server parity.
    from par_rt_db import Migration, Mutation

    c = _migrate_client()
    c.mutate(Mutation.builder().insert("users", {"name": "carol", "age": 1, "status": "x"}).build())
    assert len(c.collect_all("users")) == 3
    directives = Migration.builder().drop_field("users", "status").build().directives
    result = c.migrate_schema(directives)
    assert result.directives[0].affected_rows == 1


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


# --- FM-29: workflows + tick advancement --------------------------------------


def _wf_txn(name: str) -> Any:
    return Mutation.builder().insert("items", {"name": name, "status": "todo", "order": 1}).build()


def _wf_step(txn: Any, **kw: Any) -> Any:
    return WorkflowStepSpec(txn=txn.model_dump(by_alias=True), **kw)


def _wf_fail_step(**kw: Any) -> Any:
    txn = Mutation.builder().delete("items", "missing").build()
    return WorkflowStepSpec(txn=txn.model_dump(by_alias=True), **kw)


def _wf(name: str, steps: list[Any]) -> Any:
    return WorkflowSpec(name=name, steps=steps)


def _wf_status(c: Any, wid: str) -> Any:
    return next(w for w in c.list_workflows() if w.id == wid)


def test_workflow_step_one_succeeds_in_one_tick() -> None:
    c, clock = _new_clock_client()
    wid = c.start_workflow(_wf("single", [_wf_step(_wf_txn("a"))]))
    assert is_hex_id(wid)
    info = _wf_status(c, wid)
    assert info.status == "pending" and info.step_count == 1 and info.current_step == 0
    # sleepUntil is always set at insert (NOT NULL server column): an absent
    # sleepBeforeMs gates at the insert instant, i.e. due immediately.
    assert info.sleep_until == clock[0]
    c.tick()
    docs = c.run_query(TableQuery("items").build())
    assert [d["name"] for d in docs] == ["a"]
    assert _wf_status(c, wid).status == "success"


def test_workflow_two_steps_advance_across_sleep_gate() -> None:
    c, clock = _new_clock_client()
    wid = c.start_workflow(
        _wf("drip", [_wf_step(_wf_txn("a")), _wf_step(_wf_txn("b"), sleep_before_ms=1000)])
    )
    c.tick()  # fires step 1 only; step 2 gated at t0+1000
    assert [d["name"] for d in c.run_query(TableQuery("items").build())] == ["a"]
    info = _wf_status(c, wid)
    assert info.status == "pending" and info.current_step == 1
    assert info.sleep_until == clock[0] + 1000
    c.tick()  # still before the gate
    assert len(c.run_query(TableQuery("items").build())) == 1
    clock[0] += 1000
    c.tick()  # gate due -> step 2 fires -> terminal
    assert sorted(d["name"] for d in c.run_query(TableQuery("items").build())) == ["a", "b"]
    assert _wf_status(c, wid).status == "success"


def test_workflow_initial_sleep_before_ms_gates_first_step() -> None:
    c, clock = _new_clock_client()
    wid = c.start_workflow(_wf("delayed", [_wf_step(_wf_txn("a"), sleep_before_ms=500)]))
    c.tick()  # initial gate not yet due
    assert c.run_query(TableQuery("items").build()) == []
    assert _wf_status(c, wid).status == "pending"
    clock[0] += 500
    c.tick()
    assert len(c.run_query(TableQuery("items").build())) == 1


def test_workflow_no_sleep_chain_completes_in_one_tick() -> None:
    c, _clock = _new_clock_client()
    wid = c.start_workflow(_wf("chain", [_wf_step(_wf_txn("a")), _wf_step(_wf_txn("b"))]))
    c.tick()  # both steps in one turn — no gate between them
    assert sorted(d["name"] for d in c.run_query(TableQuery("items").build())) == ["a", "b"]
    assert _wf_status(c, wid).status == "success"


def test_workflow_retry_backoff_then_success() -> None:
    c, clock = _new_clock_client()
    # expectAbsent on by_name=["a"] fails while "a" exists; deleting it before
    # the retry gate lets attempt 2 succeed.
    c.mutate(
        Mutation.builder().insert("items", {"name": "a", "status": "todo", "order": 1}).build(),
        mut_id="seed",
    )
    step_txn = Mutation.builder().expect_absent("items", "by_name", ["a"]).build()
    wid = c.start_workflow(
        _wf(
            "retry",
            [
                _wf_step(
                    step_txn,
                    retry=StepRetry(max_attempts=3, initial_retry_ms=100, max_retry_ms=60_000),
                )
            ],
        )
    )
    c.tick()  # attempt 1 fails (PRECONDITION_FAILED) -> pending retry at t0+100
    info = _wf_status(c, wid)
    assert info.status == "pending" and info.attempts == 1
    assert info.sleep_until == clock[0] + 100
    c.tick()  # backoff not yet due
    assert _wf_status(c, wid).attempts == 1
    clock[0] += 100
    doc = c.run_query(TableQuery("items").build())[0]
    c.mutate(Mutation.builder().delete("items", doc["_id"]).build(), mut_id="rm")
    c.tick()  # attempt 2: no matching doc -> succeeds -> terminal
    info = _wf_status(c, wid)
    assert info.status == "success" and info.attempts == 0
    # attempts reset on the successful re-run boundary: the row-level attempts
    # counter reflects the CURRENT step's attempt count (0 after success
    # bookkeeping in record_step_success); the per-attempt trail lives in
    # stepOutcomes, not exposed by list_workflows.


def test_workflow_retry_exhaustion_marks_failed_with_outcome() -> None:
    c, clock = _new_clock_client()
    wid = c.start_workflow(
        _wf(
            "doomed",
            [_wf_fail_step(retry=StepRetry(max_attempts=2, initial_retry_ms=1, max_retry_ms=1))],
        )
    )
    # 1ms backoff: tick 1 fails attempt 1 and releases to pending at now+1
    # (each tick runs one claim pass, mirroring the server's scheduler poll
    # cadence); advancing the clock past the gate lets tick 2 claim, fail
    # attempt 2, and exhaust (attempts == maxAttempts) -> failed.
    c.tick()
    assert _wf_status(c, wid).attempts == 1
    clock[0] += 1
    c.tick()
    info = _wf_status(c, wid)
    assert info.status == "failed"
    assert info.attempts == 2
    assert info.last_error is not None and "not found" in info.last_error
    assert info.finished_at is not None


def test_workflow_failed_step_rolls_back_its_partial_writes() -> None:
    c, _clock = _new_clock_client()
    # Step txn: insert ok, then delete-missing fails -> whole txn rolls back.
    txn = (
        Mutation.builder()
        .insert("items", {"name": "ghost", "status": "todo", "order": 1})
        .delete("items", "missing")
        .build()
    )
    wid = c.start_workflow(
        _wf(
            "rollback",
            [_wf_step(txn, retry=StepRetry(max_attempts=1, initial_retry_ms=1, max_retry_ms=1))],
        )
    )
    c.tick()
    assert c.run_query(TableQuery("items").build()) == []
    assert _wf_status(c, wid).status == "failed"


def test_cancel_workflow_stops_a_pending_run() -> None:
    c, clock = _new_clock_client()
    wid = c.start_workflow(
        _wf("drip", [_wf_step(_wf_txn("a")), _wf_step(_wf_txn("b"), sleep_before_ms=1000)])
    )
    c.tick()  # step 1 done, step 2 pending at gate
    assert c.cancel_workflow(wid) is True
    clock[0] += 5000
    c.tick()  # cancelled runs never advance
    assert len(c.run_query(TableQuery("items").build())) == 1
    assert _wf_status(c, wid).status == "cancelled"
    # Cancelling again (terminal) is a no-op false, not an error.
    assert c.cancel_workflow(wid) is False


def test_list_workflows_filters_by_status() -> None:
    c, _clock = _new_clock_client()
    c.start_workflow(_wf("done", [_wf_step(_wf_txn("a"))]))
    wid2 = c.start_workflow(
        _wf(
            "doomed",
            [_wf_fail_step(retry=StepRetry(max_attempts=1, initial_retry_ms=1, max_retry_ms=1))],
        )
    )
    c.tick()
    assert [w.name for w in c.list_workflows(status="success")] == ["done"]
    assert [w.id for w in c.list_workflows(status="failed")] == [wid2]
    assert len(c.list_workflows()) == 2


def test_start_workflow_step_enqueues_run_fired_by_tick() -> None:
    c, _clock = _new_clock_client()
    spec = _wf("nested", [_wf_step(_wf_txn("a"))])
    m = Mutation.builder().start_workflow(spec).build()
    res = c.mutate(m)[0]
    assert res is not None
    wid = str(res.model_dump(by_alias=True)["workflowId"])
    assert is_hex_id(wid)
    c.tick()
    assert len(c.run_query(TableQuery("items").build())) == 1
    assert _wf_status(c, wid).status == "success"


def test_cancel_workflow_step_flips_pending_run() -> None:
    c, clock = _new_clock_client()
    wid = c.start_workflow(
        _wf("drip", [_wf_step(_wf_txn("a")), _wf_step(_wf_txn("b"), sleep_before_ms=1000)])
    )
    c.tick()  # step 1 done; step 2 pending
    res = c.mutate(Mutation.builder().cancel_workflow(wid).build())[0]
    assert res is not None
    assert res.model_dump()["cancelled"] is True
    clock[0] += 5000
    c.tick()
    assert _wf_status(c, wid).status == "cancelled"
    # Cancelling a terminal run through the step reports false, not an error.
    res2 = c.mutate(Mutation.builder().cancel_workflow(wid).build())[0]
    assert res2 is not None
    assert res2.model_dump()["cancelled"] is False


def test_workflow_spec_validation_rejects_invalid_specs() -> None:
    c, _clock = _new_clock_client()

    def rejects(spec: Any, message: str) -> None:
        with pytest.raises(RtDbError) as ei:
            c.start_workflow(spec)
        assert ei.value.code is ErrorCode.BAD_REQUEST
        assert ei.value.message == message

    rejects(_wf("empty", []), "workflow must have at least one step")
    rejects(
        _wf("too-many", [_wf_step(_wf_txn(f"s{i}")) for i in range(65)]),
        "workflow exceeds 64 steps",
    )
    rejects(
        _wf("zero-attempts", [_wf_step(_wf_txn("a"), retry=StepRetry(max_attempts=0))]),
        "steps[0].retry.maxAttempts must be >= 1",
    )
    rejects(
        _wf(
            "zero-initial",
            [_wf_step(_wf_txn("a"), retry=StepRetry(max_attempts=2, initial_retry_ms=0))],
        ),
        "steps[0].retry requires initialRetryMs > 0 and maxRetryMs >= initialRetryMs",
    )
    rejects(
        _wf(
            "inverted-cap",
            [
                _wf_step(
                    _wf_txn("a"),
                    retry=StepRetry(max_attempts=2, initial_retry_ms=500, max_retry_ms=100),
                )
            ],
        ),
        "steps[0].retry requires initialRetryMs > 0 and maxRetryMs >= initialRetryMs",
    )


def test_workflow_spec_nested_start_workflow_cannot_smuggle_past_max_steps() -> None:
    c, _clock = _new_clock_client()
    two_step_txn = (
        Mutation.builder()
        .insert("items", {"name": "a", "status": "todo", "order": 1})
        .insert("items", {"name": "b", "status": "todo", "order": 2})
        .build()
    )
    inner = WorkflowSpec(
        name="inner",
        steps=[
            WorkflowStepSpec(txn=two_step_txn.model_dump(by_alias=True))
            for _ in range(MAX_STEPS // 2)
        ],
    )
    outer_txn = (
        Mutation.builder()
        .insert("items", {"name": "x", "status": "todo", "order": 1})
        .start_workflow(inner)
        .build()
    )
    # The outer spec is flat-tiny (one step, a 2-step txn), but the nested
    # spec's txns push the recursive count to 2 + (MAX_STEPS // 2) * 2 =
    # 2 + MAX_STEPS > MAX_STEPS — only the recursive counter catches the
    # smuggle. Workflow steps themselves are control flow: worst_case_affected
    # counts 0 documents.
    control_only = Mutation.builder().start_workflow(inner).cancel_workflow("wf-x").build()
    assert worst_case_affected(control_only) == 0
    with pytest.raises(RtDbError) as ei:
        c.start_workflow(_wf("smuggle", [_wf_step(outer_txn)]))
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert f"recursive step count {2 + MAX_STEPS} exceeds MAX_STEPS {MAX_STEPS}" in ei.value.message
