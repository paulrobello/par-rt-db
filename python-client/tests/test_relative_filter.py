"""Execution-time-relative ``olderThan`` predicates in by-query steps (mirror
of ``server/tests/relative_filter_test.rs``): the filter op whose cutoff
(``now − ms``) is derived from the engine's injected clock at execution —
per fire for a scheduled txn — instead of a literal frozen at schedule time.
Pins the by-query-only acceptance boundary and the deterministic match
margins: OLD (1) is below any cutoff for centuries, FUTURE (9e15) is above
it, so the clock's exact value never matters."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import TypeAdapter

from par_rt_db import Mutation, StepResult, TableQuery
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.schema import Schema, t
from par_rt_db.wire import FilterExpr

#: Below ``now − SWEEP_MS`` for centuries (epoch-ms today is ~1.8e12; the
#: cutoff is ~0.8e12 and rising by 1/year).
OLD = 1
#: 9e15 — above ``now − 0`` effectively forever; f64-exact, within i64.
FUTURE = 9_000_000_000_000_000
SWEEP_MS = 1_000_000_000_000

_flt = TypeAdapter(FilterExpr)


def _older_than(field: str, ms: int) -> Any:
    return _flt.validate_python({"op": "olderThan", "field": field, "ms": ms})


def _number_schema() -> Any:
    return (
        Schema.builder()
        .table(
            "tasks",
            lambda tb: (
                tb.field("title", t.string())
                .field("updatedAt", t.number())
                .index("by_title", ["title"])
            ),
        )
        .build()
    )


def _int64_indexed_schema() -> Any:
    """``updatedAt`` as int64 and indexed, so scans take the typed-column path
    (decimal-string wire form end to end)."""
    return (
        Schema.builder()
        .table(
            "tasks",
            lambda tb: (
                tb.field("title", t.string())
                .field("updatedAt", t.int64())
                .index("by_title", ["title"])
                .index("by_updatedAt", ["updatedAt"])
            ),
        )
        .build()
    )


def _new_client(schema: Any) -> InMemoryRtDbClient:
    # Injected epoch-millis clock ~1.7e12 (the corpus/golden-vector
    # convention) — the by-query evaluation must read it at execution.
    counter = [1_700_000_000_000]

    def now() -> int:
        counter[0] += 1
        return counter[0]

    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.0))
    c.push_schema(schema)
    return c


def _seed(c: InMemoryRtDbClient, title: str, updated_at: Any) -> None:
    [res] = c.mutate(
        Mutation.builder().insert("tasks", {"title": title, "updatedAt": updated_at}).build()
    )
    assert isinstance(res, StepResult)


def _count_titles(c: InMemoryRtDbClient, title: str) -> int:
    docs = c.run_query(TableQuery("tasks").with_index("by_title").eq(title).collect().build())
    return len(docs)


def test_patch_by_query_older_than_patches_old_rows_only() -> None:
    c = _new_client(_number_schema())
    _seed(c, "old", OLD)
    _seed(c, "future", FUTURE)

    [res] = c.mutate(
        Mutation.builder()
        .patch_by_query("tasks", _older_than("updatedAt", SWEEP_MS), {"title": "swept"})
        .build()
    )
    assert res is not None
    assert res.model_dump(by_alias=True) == {"patched": 1, "truncated": False}
    assert _count_titles(c, "swept") == 1
    assert _count_titles(c, "future") == 1


def test_delete_by_query_older_than_deletes_old_rows_only() -> None:
    c = _new_client(_number_schema())
    _seed(c, "old", OLD)
    _seed(c, "future", FUTURE)

    [res] = c.mutate(
        Mutation.builder().delete_by_query("tasks", _older_than("updatedAt", SWEEP_MS)).build()
    )
    assert res is not None
    assert res.model_dump(by_alias=True) == {"deleted": 1, "truncated": False}
    assert _count_titles(c, "old") == 0
    assert _count_titles(c, "future") == 1


def test_patch_by_query_older_than_takes_the_int64_column_path() -> None:
    c = _new_client(_int64_indexed_schema())
    # int64 wire form is a decimal string.
    _seed(c, "old", str(OLD))
    _seed(c, "future", str(FUTURE))

    [res] = c.mutate(
        Mutation.builder()
        .patch_by_query("tasks", _older_than("updatedAt", SWEEP_MS), {"title": "swept"})
        .build()
    )
    assert res is not None
    assert res.model_dump(by_alias=True) == {"patched": 1, "truncated": False}
    assert _count_titles(c, "future") == 1
    # The future row survives with its decimal-string value untouched.
    [doc] = c.run_query(TableQuery("tasks").with_index("by_title").eq("future").collect().build())
    assert doc["updatedAt"] == str(FUTURE)


def test_read_query_filter_older_than_is_rejected() -> None:
    c = _new_client(_number_schema())
    with pytest.raises(RtDbError) as ei:
        c.run_query(TableQuery("tasks").filter(_older_than("updatedAt", SWEEP_MS)).build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "only allowed in patchByQuery/deleteByQuery" in ei.value.message


def test_older_than_rejects_non_numeric_field_and_negative_ms() -> None:
    schema = (
        Schema.builder()
        .table(
            "tasks",
            lambda tb: (
                tb.field("title", t.string())
                .field("updatedAt", t.string())
                .index("by_title", ["title"])
            ),
        )
        .build()
    )
    c = _new_client(schema)

    with pytest.raises(RtDbError) as ei:
        c.mutate(
            Mutation.builder()
            .patch_by_query("tasks", _older_than("updatedAt", SWEEP_MS), {"title": "swept"})
            .build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "must be a number or int64" in ei.value.message

    with pytest.raises(RtDbError) as ei:
        c.mutate(
            Mutation.builder()
            .patch_by_query("tasks", _older_than("title", -1), {"title": "swept"})
            .build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "ms must be >= 0" in ei.value.message


def test_undeclared_field_is_rejected() -> None:
    c = _new_client(_number_schema())
    with pytest.raises(RtDbError) as ei:
        c.mutate(
            Mutation.builder()
            .patch_by_query("tasks", _older_than("nope", SWEEP_MS), {"title": "swept"})
            .build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "filter references undeclared field 'nope'" in ei.value.message


def test_authorize_and_partial_index_older_than_rejected_at_push() -> None:
    authorize_pred = {"op": "olderThan", "field": "updatedAt", "ms": SWEEP_MS}
    with_authorize = (
        Schema.builder()
        .table(
            "tasks",
            lambda tb: (
                tb.field("title", t.string())
                .field("updatedAt", t.number())
                .index("by_title", ["title"])
                .authorize(_flt.validate_python(authorize_pred))
            ),
        )
        .build()
    )
    with pytest.raises(RtDbError) as ei:
        _new_client(with_authorize)
    assert ei.value.code is ErrorCode.SCHEMA_VIOLATION
    assert "only allowed in patchByQuery/deleteByQuery" in ei.value.message

    where_pred = _flt.validate_python(authorize_pred)
    with_where = (
        Schema.builder()
        .table(
            "tasks",
            lambda tb: (
                tb.field("title", t.string())
                .field("updatedAt", t.number())
                .index("by_title", ["title"])
                .index("by_updatedAt", ["updatedAt"])
                .where(where_pred)
            ),
        )
        .build()
    )
    with pytest.raises(RtDbError) as ei:
        _new_client(with_where)
    # the DDL compile path's own code, not the authorize arm's SCHEMA_VIOLATION
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "not allowed in a partial-index predicate" in ei.value.message


def test_computed_case_when_older_than_rejected_at_push() -> None:
    from par_rt_db import ve

    schema = (
        Schema.builder()
        .table(
            "tasks",
            lambda tb: (
                tb.field("title", t.string())
                .field("updatedAt", t.number())
                .field("bucket", t.string())
                .index("by_title", ["title"])
                .computed(
                    "bucket",
                    ve.case(
                        [
                            {
                                "when": {
                                    "op": "olderThan",
                                    "field": "updatedAt",
                                    "ms": SWEEP_MS,
                                },
                                "then": ve.literal("old"),
                            }
                        ],
                        ve.literal("fresh"),
                    ),
                )
            ),
        )
        .build()
    )
    with pytest.raises(RtDbError) as ei:
        _new_client(schema)
    # validate_computed maps to BAD_REQUEST, matching the read-filter rejection
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "only allowed in patchByQuery/deleteByQuery" in ei.value.message


def test_older_than_composes_inside_and_or_not() -> None:
    c = _new_client(_number_schema())
    _seed(c, "old", OLD)
    _seed(c, "future", FUTURE)

    # and: both must hold (mirrors the wire-corpus m11 delete step's shape).
    flt = _flt.validate_python(
        {
            "op": "and",
            "exprs": [
                {"op": "olderThan", "field": "updatedAt", "ms": SWEEP_MS},
                {"op": "exists", "field": "updatedAt"},
            ],
        }
    )
    [res] = c.mutate(Mutation.builder().delete_by_query("tasks", flt).build())
    assert res is not None
    assert res.model_dump(by_alias=True) == {"deleted": 1, "truncated": False}

    # not: inverts — the surviving future row matches not(olderThan).
    flt = _flt.validate_python(
        {"op": "not", "expr": {"op": "olderThan", "field": "updatedAt", "ms": SWEEP_MS}}
    )
    [res] = c.mutate(Mutation.builder().patch_by_query("tasks", flt, {"title": "kept"}).build())
    assert res is not None
    assert res.model_dump(by_alias=True) == {"patched": 1, "truncated": False}


def test_older_than_null_absent_and_non_numeric_never_match() -> None:
    from par_rt_db.in_memory import _eval_filter_expr

    fields: dict[str, Any] = {"updatedAt": t.number(), "n64": t.int64()}
    expr = _older_than("updatedAt", SWEEP_MS)
    # 1.7e12 clock: the cutoff is ~0.7e12.
    assert _eval_filter_expr(expr, {"updatedAt": 1}, fields, 1_700_000_000_000) is True
    assert _eval_filter_expr(expr, {"updatedAt": None}, fields, 1_700_000_000_000) is False
    assert _eval_filter_expr(expr, {}, fields, 1_700_000_000_000) is False
    assert _eval_filter_expr(expr, {"updatedAt": True}, fields, 1_700_000_000_000) is False
    assert _eval_filter_expr(expr, {"updatedAt": "nope"}, fields, 1_700_000_000_000) is False
    # A non-ASCII numeric string is not a strict decimal — never matches.
    assert _eval_filter_expr(expr, {"updatedAt": "١٢٣"}, fields, 1_700_000_000_000) is False
    # int64 decimal strings parse and compare exactly; the grammar is
    # ASCII-strict (a Unicode-digit string is not an i64 decimal — mirrors
    # Rust ``i64::from_str``).
    assert _eval_filter_expr(_older_than("n64", 0), {"n64": "1"}, fields, 1_700_000_000_000) is True
    assert (
        _eval_filter_expr(_older_than("n64", 0), {"n64": "x"}, fields, 1_700_000_000_000) is False
    )
    assert (
        _eval_filter_expr(_older_than("n64", 0), {"n64": "١٢٣"}, fields, 1_700_000_000_000) is False
    )
    # Without a clock (the read-path default) the leaf fail-closes.
    assert _eval_filter_expr(expr, {"updatedAt": 1}, fields) is False
