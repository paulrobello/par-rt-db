"""Tests for computed fields (ENH-028) — the ``ValueExpr`` interpreter, the
schema DSL / wire shape, push validation, write-path stamping in the in-memory
harness, and the migrate interplay.

Mirrors ``server/src/value_expr.rs``'s interpreter unit tests and
``server/tests/computed_test.rs`` at engine level: the semantics table's edge
rules (null propagation before the divide-by-zero check, trim stripping spaces
only, the cast error paths, ``toBoolean``'s Postgres literal word set), the
six push-validation rules, the stamp's authority model (client-supplied values
dropped before validation, a null result REMOVES the key), and renameField's
expression rewrite.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import TypeAdapter

from par_rt_db import CaseWhen, Mutation, Schema, t, ve
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.in_memory.value_expr import (
    eval_value_expr,
    walk_value_expr_fields,
)
from par_rt_db.schema import SchemaDef
from par_rt_db.value_expr import ValueExpr
from par_rt_db.wire import FilterExpr

_VE = TypeAdapter(ValueExpr)
_FLT = TypeAdapter(FilterExpr)
_TABLE = "users"


def _e(expr: dict[str, Any]) -> ValueExpr:
    """Validate a raw ``ve.*`` dict into the ``ValueExpr`` union."""
    return _VE.validate_python(expr)


def _eval(expr: dict[str, Any], doc: dict[str, Any], now: int = 0) -> Any:
    """Evaluate a raw expression dict over ``doc`` (type-less fields — the
    engine passes the table's field map only for ``Case`` int64 filters)."""
    return eval_value_expr(_e(expr), doc, now, {})


# ---------------------------------------------------------------------------
# Interpreter — the semantics table's edge rules (mirrors server value_expr.rs)
# ---------------------------------------------------------------------------


def test_field_reads_are_text_and_absent_is_null() -> None:
    doc = {
        "s": "x",
        "n": 42,
        "f": 42.5,
        "b": True,
        "o": {"a": 1},
        "nil": None,
    }
    assert _eval(ve.field("s"), doc) == "x"
    assert _eval(ve.field("n"), doc) == "42"
    assert _eval(ve.field("f"), doc) == "42.5"
    assert _eval(ve.field("b"), doc) == "true"
    # Objects extract as COMPACT JSON text ({"a":1} — the pinned convention,
    # deliberately not Postgres's spaced jsonb text).
    assert _eval(ve.field("o"), doc) == '{"a":1}'
    assert _eval(ve.field("nil"), doc) is None
    assert _eval(ve.field("missing"), doc) is None


def test_literal_passes_through() -> None:
    for v in ("s", 42, 42.5, True, {"a": [1, 2]}, None):
        assert _eval(ve.literal(v), {}) == v


def test_concat_skips_nulls_and_casts_numbers_to_text() -> None:
    doc = {"first": "Ada", "n": 42}
    expr = ve.concat(ve.field("first"), ve.field("missing"), ve.field("n"))
    assert _eval(expr, doc) == "Ada42"


def test_concat_all_null_parts_is_empty_string() -> None:
    expr = ve.concat(ve.field("missing"), ve.literal(None))
    assert _eval(expr, {}) == ""


def test_add_coerces_string_fields_to_numeric() -> None:
    doc = {"a": "42", "b": "1"}
    assert _eval(ve.add(ve.field("a"), ve.field("b")), doc) == 43.0


def test_arithmetic_propagates_null_over_operands() -> None:
    one = ve.literal(1)
    missing = ve.field("missing")
    for expr in (
        ve.add(missing, one),
        ve.sub(one, missing),
        ve.mul(missing, one),
        ve.div(one, missing),
    ):
        assert _eval(expr, {}) is None
    # Null precedes the zero check: null / 0 is null, not an error.
    assert _eval(ve.div(missing, ve.literal(0)), {}) is None


def test_div_by_zero_errors() -> None:
    with pytest.raises(RtDbError) as ei:
        _eval(ve.div(ve.literal(1), ve.literal(0)), {})
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert ei.value.message == "division by zero"
    # -0.0 is the same divisor error (IEEE equality).
    with pytest.raises(RtDbError, match="division by zero"):
        _eval(ve.div(ve.literal(1), ve.literal(-0.0)), {})


def test_div_non_finite_result_errors() -> None:
    with pytest.raises(RtDbError) as ei:
        _eval(ve.div(ve.literal(1e308), ve.literal(1e-10)), {})
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert ei.value.message == "numeric result is not finite"


def test_div_happy_path_is_float() -> None:
    assert _eval(ve.div(ve.literal(1), ve.literal(4)), {}) == 0.25


def test_coalesce_returns_first_non_null_else_null() -> None:
    assert _eval(ve.coalesce(ve.field("missing"), ve.literal(7)), {}) == 7
    assert _eval(ve.coalesce(ve.field("a"), ve.field("b")), {}) is None


def test_lower_upper_trim() -> None:
    doc = {"mixed": "MiXeD", "padded": "  x  ", "tabbed": "  \tx  "}
    assert _eval(ve.lower(ve.field("mixed")), doc) == "mixed"
    assert _eval(ve.upper(ve.field("mixed")), doc) == "MIXED"
    assert _eval(ve.trim(ve.field("padded")), doc) == "x"
    # Spaces only — the tab survives btrim's default.
    assert _eval(ve.trim(ve.field("tabbed")), doc) == "\tx"
    assert _eval(ve.lower(ve.field("missing")), doc) is None


def test_cast_to_string_uses_text_extraction() -> None:
    doc = {"n": 42, "o": {"a": 1}}
    from par_rt_db import Cast

    assert _eval(ve.cast(ve.field("n"), Cast.TO_STRING), doc) == "42"
    assert _eval(ve.cast(ve.field("o"), Cast.TO_STRING), doc) == '{"a":1}'
    assert _eval(ve.cast(ve.field("missing"), Cast.TO_STRING), doc) is None


def test_cast_to_number_parses_trimmed_strings() -> None:
    from par_rt_db import Cast

    doc = {"s": "  3.5 ", "bad": "abc", "b": True}
    assert _eval(ve.cast(ve.field("s"), Cast.TO_NUMBER), doc) == 3.5
    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.cast(ve.field("bad"), Cast.TO_NUMBER), doc)
    # A bool FIELD reaches the cast as its text form ("true"), so it fails the
    # string parse; a bool LITERAL hits the type-error arm directly.
    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.cast(ve.field("b"), Cast.TO_NUMBER), doc)
    with pytest.raises(RtDbError) as ei:
        _eval(ve.cast(ve.literal(True), Cast.TO_NUMBER), {})
    assert ei.value.message == "cannot cast to number"
    assert _eval(ve.cast(ve.field("missing"), Cast.TO_NUMBER), doc) is None


def test_numeric_string_with_underscores_is_rejected() -> None:
    # Python's float() accepts PEP 515 underscores ("1_0" → 10.0); Rust's
    # f64::from_str errors, so the strict grammar rejects them (mirrors the
    # ts engine's parseNumericString).
    from par_rt_db import Cast

    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.cast(ve.literal("1_0"), Cast.TO_NUMBER), {})
    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.add(ve.literal("1_0"), ve.literal(1)), {})


def test_cast_to_int64_requires_integral_numbers() -> None:
    from par_rt_db import Cast

    doc = {"i": 42, "float": 3.5, "s": "  7 ", "bad": "8x", "b": True}
    assert _eval(ve.cast(ve.field("i"), Cast.TO_INT64), doc) == 42
    assert _eval(ve.cast(ve.field("s"), Cast.TO_INT64), doc) == 7
    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.cast(ve.field("float"), Cast.TO_INT64), doc)
    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.cast(ve.field("bad"), Cast.TO_INT64), doc)
    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.cast(ve.field("b"), Cast.TO_INT64), doc)
    with pytest.raises(RtDbError) as ei:
        _eval(ve.cast(ve.literal(True), Cast.TO_INT64), {})
    assert ei.value.message == "cannot cast to int64"
    assert _eval(ve.cast(ve.field("missing"), Cast.TO_INT64), doc) is None


def test_cast_to_boolean_accepts_postgres_literal_set() -> None:
    from par_rt_db import Cast

    doc = {"b": True, "two": 2}
    assert _eval(ve.cast(ve.field("b"), Cast.TO_BOOLEAN), doc) is True
    assert _eval(ve.cast(ve.literal(1), Cast.TO_BOOLEAN), doc) is True
    assert _eval(ve.cast(ve.literal(0), Cast.TO_BOOLEAN), doc) is False
    for word, want in (
        ("TRUE", True),
        ("t", True),
        ("Yes", True),
        ("on", True),
        ("1", True),
        ("False", False),
        ("f", False),
        ("No", False),
        ("OFF", False),
        ("0", False),
    ):
        assert _eval(ve.cast(ve.literal(word), Cast.TO_BOOLEAN), doc) is want
    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.cast(ve.literal("maybe"), Cast.TO_BOOLEAN), doc)
    with pytest.raises(RtDbError, match="cannot cast"):
        _eval(ve.cast(ve.field("two"), Cast.TO_BOOLEAN), doc)
    assert _eval(ve.cast(ve.field("missing"), Cast.TO_BOOLEAN), doc) is None


def test_now_yields_epoch_ms_as_number() -> None:
    assert _eval(ve.now(), {}, now=1_234_567_890) == 1_234_567_890
    assert isinstance(_eval(ve.now(), {}, now=5), int)


def test_case_takes_first_match_then_otherwise() -> None:
    from par_rt_db import Cast

    doc = {"status": "admin", "n": 5}
    expr = ve.case(
        [
            {"when": {"op": "eq", "field": "status", "value": "user"}, "then": ve.literal(1)},
            {"when": {"op": "eq", "field": "status", "value": "admin"}, "then": ve.literal(2)},
        ],
        ve.literal(4),
    )
    assert _eval(expr, doc) == 2
    unmatched = ve.case(
        [{"when": {"op": "gt", "field": "n", "value": 10}, "then": ve.literal(3)}],
        ve.field("status"),
    )
    assert _eval(unmatched, doc) == "admin"
    # A nested case inside a then-branch, and marker values are push-rejected
    # (so any ctx is semantically irrelevant here).
    nested = ve.case(
        [
            {
                "when": {"op": "exists", "field": "n"},
                "then": ve.cast(ve.field("n"), Cast.TO_STRING),
            }
        ],
        ve.literal("?"),
    )
    assert _eval(nested, doc) == "5"


def test_walk_visits_fields_and_case_when_fields() -> None:
    expr = _e(
        ve.concat(
            ve.field("a"),
            ve.case(
                [
                    {
                        "when": {
                            "op": "and",
                            "exprs": [
                                {"op": "eq", "field": "b", "value": 1},
                                {
                                    "op": "not",
                                    "expr": {
                                        "op": "contains",
                                        "field": "c",
                                        "value": "x",
                                    },
                                },
                            ],
                        },
                        "then": ve.field("d"),
                    },
                    {
                        "when": {"op": "exists", "field": "e"},
                        "then": ve.literal(1),
                    },
                ],
                ve.field("f"),
            ),
            ve.add(ve.field("g"), ve.div(ve.field("h"), ve.coalesce(ve.field("i")))),
        )
    )
    seen: list[str] = []
    walk_value_expr_fields(expr, seen.append)
    assert sorted(seen) == ["a", "b", "c", "d", "e", "f", "g", "h", "i"]


# ---------------------------------------------------------------------------
# DSL + wire shape
# ---------------------------------------------------------------------------


def test_builder_computed_wire_shape_and_omission() -> None:
    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("first", t.string())
                .field("last", t.string())
                .field("fullName", t.string())
                .index("by_fullName", ["fullName"])
                .computed(
                    "fullName",
                    ve.concat(ve.field("first"), ve.literal(" "), ve.field("last")),
                )
            ),
        )
        .build()
    )
    dumped = schema.model_dump(by_alias=True, mode="json")
    assert dumped["tables"][_TABLE]["computed"] == {
        "fullName": {
            "op": "concat",
            "parts": [
                {"op": "field", "field": "first"},
                {"op": "literal", "value": " "},
                {"op": "field", "field": "last"},
            ],
        }
    }
    # Round-trips through the wire model.
    assert SchemaDef.model_validate(dumped) == schema
    # A table with no computed entries omits the key entirely.
    plain = Schema.builder().table(_TABLE, lambda tb: tb.field("name", t.string())).build()
    assert "computed" not in plain.model_dump(by_alias=True, mode="json")["tables"][_TABLE]


def test_value_expr_rejects_unknown_ops_and_fields() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _VE.validate_python({"op": "explode"})
    _VE.validate_python({"op": "field", "field": "x"})  # baseline sanity
    with pytest.raises(ValidationError):
        _VE.validate_python({"op": "field", "field": "x", "bogus": 1})


def test_casewhen_model_is_exported_and_usable() -> None:
    cw = CaseWhen.model_validate(
        {"when": {"op": "eq", "field": "n", "value": 1}, "then": ve.literal("one")}
    )
    assert _eval(ve.case([cw], ve.literal("?")), {"n": 1}) == "one"


# ---------------------------------------------------------------------------
# Push validation (the six rules — port of server schema.rs::validate_computed)
# ---------------------------------------------------------------------------


def _push(schema_json: dict[str, Any]) -> None:
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 0))
    c.push_schema(Schema.model_validate({"tables": {_TABLE: schema_json}}))


def _users_table(**extra: Any) -> dict[str, Any]:
    table: dict[str, Any] = {
        "fields": {
            "first": {"type": "string"},
            "last": {"type": "string"},
            "fullName": {"type": "string"},
        },
        "indexes": [{"name": "by_fullName", "fields": ["fullName"]}],
    }
    table.update(extra)
    return table


def test_push_rejects_computed_key_not_declared() -> None:
    with pytest.raises(RtDbError) as ei:
        _push(
            _users_table(
                computed={"nickname": ve.upper(ve.field("first"))},
            )
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "not a declared field" in ei.value.message


def test_push_rejects_computed_on_stamped_declaration_fields() -> None:
    from par_rt_db import Cast

    stamped = {"ownerField": "uid", "collaboratorsField": "collabs", "autoIncrementField": "num"}
    fields = {
        "uid": {"type": "string"},
        "collabs": {"type": "array", "element": {"type": "string"}},
        "num": {"type": "int64"},
    }
    for which, key in stamped.items():
        table = {
            "fields": fields,
            which: key,
            "computed": {key: ve.cast(ve.field("uid"), Cast.TO_STRING)},
        }
        with pytest.raises(RtDbError) as ei:
            _push(table)
        assert ei.value.code is ErrorCode.BAD_REQUEST
        assert f"must not be the table's {which}" in ei.value.message


def test_push_rejects_undeclared_reference() -> None:
    with pytest.raises(RtDbError) as ei:
        _push(
            _users_table(
                computed={
                    "fullName": ve.concat(ve.field("first"), ve.field("middle")),
                },
            )
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "references undeclared field 'middle'" in ei.value.message


def test_push_rejects_computed_referencing_computed() -> None:
    table = _users_table(
        computed={
            "fullName": ve.concat(ve.field("first"), ve.field("last")),
            "upperName": ve.upper(ve.field("fullName")),
        },
    )
    table["fields"]["upperName"] = {"type": "string"}
    with pytest.raises(RtDbError) as ei:
        _push(table)
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "references computed field 'fullName'" in ei.value.message


def test_push_rejects_principal_markers_in_case_whens() -> None:
    with pytest.raises(RtDbError) as ei:
        _push(
            _users_table(
                computed={
                    "fullName": ve.case(
                        [
                            {
                                "when": {
                                    "op": "eq",
                                    "field": "first",
                                    "value": {"$user": True},
                                },
                                "then": ve.field("last"),
                            }
                        ],
                        ve.field("first"),
                    ),
                },
            )
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "principal markers" in ei.value.message


def test_push_rejects_case_when_reference_to_undeclared_field() -> None:
    # Rule 3's walk covers Case.when filter fields too.
    with pytest.raises(RtDbError) as ei:
        _push(
            _users_table(
                computed={
                    "fullName": ve.case(
                        [
                            {
                                "when": {"op": "eq", "field": "middle", "value": "x"},
                                "then": ve.field("first"),
                            }
                        ],
                        ve.field("last"),
                    ),
                },
            )
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "references undeclared field 'middle'" in ei.value.message


def test_push_static_kind_rejections() -> None:
    from par_rt_db import Cast

    # concat (String kind) into a number field.
    with pytest.raises(RtDbError) as ei:
        _push(
            {
                "fields": {
                    "denom": {"type": "optional", "inner": {"type": "number"}},
                    "ratio": {"type": "optional", "inner": {"type": "number"}},
                },
                "indexes": [{"name": "by_denom", "fields": ["denom"]}],
                "computed": {"ratio": ve.concat(ve.field("denom"), ve.literal("x"))},
            }
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "produces a string" in ei.value.message
    # arithmetic (Number kind) into an int64 field — an int64's wire form is a
    # decimal string, so a JSON number can never validate.
    with pytest.raises(RtDbError, match="produces a number"):
        _push(
            {
                "fields": {"a": {"type": "int64"}, "b": {"type": "int64"}},
                "indexes": [{"name": "by_a", "fields": ["a"]}],
                "computed": {"b": ve.add(ve.cast(ve.field("a"), Cast.TO_NUMBER), ve.literal(1))},
            }
        )
    # lower (String kind) into a boolean field.
    with pytest.raises(RtDbError, match="produces a string"):
        _push(
            {
                "fields": {"s": {"type": "string"}, "flag": {"type": "boolean"}},
                "indexes": [{"name": "by_s", "fields": ["s"]}],
                "computed": {"flag": ve.cast(ve.lower(ve.field("s")), Cast.TO_STRING)},
            }
        )


def test_push_static_kind_accepts_the_canonical_shapes() -> None:
    from par_rt_db import Cast

    # concat on string; arithmetic on number; Now on number; Cast(ToString)
    # into int64 (the decimal-string possibility); Cast(ToBoolean) on boolean;
    # optional wrappers admit the nullable spelling; Field/Coalesce/Case skip
    # the static check entirely.
    _push(
        {
            "fields": {
                "first": {"type": "string"},
                "last": {"type": "string"},
                "nick": {"type": "optional", "inner": {"type": "string"}},
                "fullName": {"type": "string"},
                "slug": {"type": "optional", "inner": {"type": "string"}},
                "score": {"type": "number"},
                "views": {"type": "int64"},
                "seenAt": {"type": "number"},
                "flag": {"type": "boolean"},
                "status": {"type": "string"},
                "label": {"type": "any"},
            },
            "indexes": [{"name": "by_fullName", "fields": ["fullName"]}],
            "computed": {
                "fullName": ve.concat(ve.field("first"), ve.literal(" "), ve.field("last")),
                "slug": ve.lower(ve.trim(ve.field("nick"))),
                "score": ve.add(ve.literal(1), ve.literal(2)),
                "views": ve.cast(ve.literal(42), Cast.TO_STRING),
                "seenAt": ve.now(),
                "flag": ve.cast(ve.literal("on"), Cast.TO_BOOLEAN),
                "label": ve.coalesce(ve.field("nick")),
            },
        }
    )


def test_push_rejects_authorize_referencing_computed_field() -> None:
    with pytest.raises(RtDbError) as ei:
        _push(
            _users_table(
                authorize={"op": "eq", "field": "fullName", "value": "public"},
                computed={
                    "fullName": ve.concat(ve.field("first"), ve.field("last")),
                },
            )
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "authorize predicate" in ei.value.message


# ---------------------------------------------------------------------------
# Write-path stamping
# ---------------------------------------------------------------------------


def _computed_client() -> tuple[InMemoryRtDbClient, list[int]]:
    """``users`` with a required concat fullName and an optional coalesce
    nick; a frozen mutable clock."""
    clock = [10_000]

    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("first", t.string())
                .field("last", t.string())
                .field("nick", t.optional(t.string()))
                .field("fullName", t.string())
                .field("badge", t.optional(t.string()))
                .index("by_fullName", ["fullName"])
                .computed(
                    "fullName", ve.concat(ve.field("first"), ve.literal(" "), ve.field("last"))
                )
                .computed("badge", ve.coalesce(ve.field("nick")))
            ),
        )
        .build()
    )
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: clock[0], random=lambda: 0.5))
    c.push_schema(schema)
    return c, clock


def _insert(c: InMemoryRtDbClient, doc: dict[str, Any]) -> str:
    [res] = c.mutate(Mutation.builder().insert(_TABLE, doc).build())
    assert res is not None
    return str(res.model_dump()["id"])


def test_insert_overwrites_client_supplied_computed_value() -> None:
    c, _ = _computed_client()
    doc_id = _insert(c, {"first": "Ada", "last": "Lovelace", "fullName": "WRONG"})
    doc = c.get(_TABLE, doc_id)
    assert doc is not None and doc["fullName"] == "Ada Lovelace"
    # An optional computed over a missing input stores NO key (null removes).
    assert "badge" not in doc


def test_patch_recomputes_over_merged_doc() -> None:
    c, _ = _computed_client()
    doc_id = _insert(c, {"first": "Gracie", "last": "Hopper"})
    c.mutate(
        Mutation.builder().patch(_TABLE, doc_id, {"first": "Grace"}).build(),
    )
    doc = c.get(_TABLE, doc_id)
    assert doc is not None and doc["fullName"] == "Grace Hopper"


def test_patch_computed_key_is_dropped_not_merged() -> None:
    c, _ = _computed_client()
    doc_id = _insert(c, {"first": "Ada", "last": "Lovelace"})
    # A patch carrying the computed key changes nothing — even a wrong-typed
    # value is silently dropped rather than failing validation.
    c.mutate(
        Mutation.builder().patch(_TABLE, doc_id, {"fullName": 3.14}).build(),
    )
    doc = c.get(_TABLE, doc_id)
    assert doc is not None and doc["fullName"] == "Ada Lovelace"


def test_patch_null_input_removes_computed_key() -> None:
    c, _ = _computed_client()
    doc_id = _insert(c, {"first": "Ada", "last": "Lovelace", "nick": "Ace"})
    doc = c.get(_TABLE, doc_id)
    assert doc is not None and doc["badge"] == "Ace"
    c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"nick": None}).build())
    doc = c.get(_TABLE, doc_id)
    assert doc is not None
    assert "nick" not in doc
    assert "badge" not in doc  # the recomputed null REMOVES the computed key


def test_replace_drops_and_restamps_computed() -> None:
    c, _ = _computed_client()
    doc_id = _insert(c, {"first": "Ada", "last": "Lovelace"})
    c.mutate(
        Mutation.builder()
        .replace(_TABLE, doc_id, {"first": "Grace", "last": "Hopper", "fullName": "WRONG"})
        .build(),
    )
    doc = c.get(_TABLE, doc_id)
    assert doc is not None and doc["fullName"] == "Grace Hopper"


def test_upsert_both_branches_restamp() -> None:
    c, _ = _computed_client()
    # The eq lookup rides the by_fullName index — the stamped value serves it.
    [res] = c.mutate(
        Mutation.builder()
        .upsert(
            _TABLE,
            "by_fullName",
            ["Ada Lovelace"],
            insert={"first": "Ada", "last": "Lovelace"},
            patch={"first": "Ada"},
        )
        .build()
    )
    assert res is not None and res.model_dump()["inserted"] is True
    [res] = c.mutate(
        Mutation.builder()
        .upsert(
            _TABLE,
            "by_fullName",
            ["Grace Hopper"],
            insert={"first": "Grace", "last": "Hopper"},
            patch={"first": "Gracie"},
        )
        .build()
    )
    assert res is not None and res.model_dump()["inserted"] is True
    # Update branch: the patch restamps fullName from the merged doc even
    # though the patch never mentions last.
    [res] = c.mutate(
        Mutation.builder()
        .upsert(
            _TABLE,
            "by_fullName",
            ["Grace Hopper"],
            insert={"first": "Grace", "last": "Hopper"},
            patch={"first": "Gracie"},
        )
        .build()
    )
    assert res is not None and res.model_dump()["inserted"] is False
    docs = {d["first"]: d for d in c.collect_all(_TABLE)}
    assert docs["Gracie"]["fullName"] == "Gracie Hopper"


def test_patch_by_query_restamps() -> None:
    c, _ = _computed_client()
    _insert(c, {"first": "Ada", "last": "Lovelace"})
    _insert(c, {"first": "Alan", "last": "Turing"})
    flt = _FLT.validate_python({"op": "eq", "field": "last", "value": "Turing"})
    c.mutate(Mutation.builder().patch_by_query(_TABLE, flt, {"first": "Al"}).build())
    docs = c.collect_all(_TABLE)
    assert all(d["fullName"] == f"{d['first']} {d['last']}" for d in docs), docs


def test_cascade_set_null_restamps_child_computed() -> None:
    schema = (
        Schema.builder()
        .table(
            "parents",
            lambda tb: tb.field("name", t.string()).index("by_name", ["name"]),
        )
        .table(
            "children",
            lambda tb: (
                tb.field("parent", t.optional(t.id("parents", "setNull")))
                .field("title", t.string())
                .field("badge", t.optional(t.string()))
                .index("by_parent", ["parent"])
                .computed("badge", ve.coalesce(ve.field("parent")))
            ),
        )
        .build()
    )
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 0, random=lambda: 0.5))
    c.push_schema(schema)
    [parent] = c.mutate(Mutation.builder().insert("parents", {"name": "p"}).build())
    parent_id = str(parent.model_dump()["id"])  # type: ignore[attr-defined]
    [child] = c.mutate(
        Mutation.builder().insert("children", {"parent": parent_id, "title": "c"}).build()
    )
    child_id = str(child.model_dump()["id"])  # type: ignore[attr-defined]
    doc = c.get("children", child_id)
    assert doc is not None and doc["badge"] == parent_id
    c.mutate(Mutation.builder().delete("parents", parent_id).build())
    doc = c.get("children", child_id)
    assert doc is not None
    assert "parent" not in doc
    assert "badge" not in doc  # recomputed over the nulled doc → key removed


def test_eval_error_fails_write_naming_field() -> None:
    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("a", t.number())
                .field("b", t.number())
                .field("q", t.optional(t.number()))
                .index("by_a", ["a"])
                .computed("q", ve.div(ve.field("a"), ve.field("b")))
            ),
        )
        .build()
    )
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 0, random=lambda: 0.5))
    c.push_schema(schema)
    doc_id = _insert(c, {"a": 1, "b": 2})
    stored = c.get(_TABLE, doc_id)
    assert stored is not None and stored["q"] == 0.5
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"b": 0}).build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "computed field 'q'" in ei.value.message
    assert "division by zero" in ei.value.message
    # The whole txn rolled back: the stored doc is untouched.
    stored = c.get(_TABLE, doc_id)
    assert stored is not None and stored["b"] == 2


def test_now_expr_uses_engine_clock() -> None:
    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("name", t.string())
                .field("seenAt", t.number())
                .index("by_name", ["name"])
                .computed("seenAt", ve.now())
            ),
        )
        .build()
    )
    clock = [10_000]
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: clock[0], random=lambda: 0.5))
    c.push_schema(schema)
    doc_id = _insert(c, {"name": "a"})
    doc = c.get(_TABLE, doc_id)
    assert doc is not None and doc["seenAt"] == 10_000
    clock[0] += 1
    c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"name": "b"}).build())
    doc = c.get(_TABLE, doc_id)
    assert doc is not None and doc["seenAt"] == 10_001


def test_int64_computed_via_cast_to_string_stores_int64_wire_form() -> None:
    from par_rt_db import Cast

    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("n", t.int64())
                .field("echo", t.int64())
                .index("by_n", ["n"])
                .computed("echo", ve.cast(ve.field("n"), Cast.TO_STRING))
            ),
        )
        .build()
    )
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 0, random=lambda: 0.5))
    c.push_schema(schema)
    doc_id = _insert(c, {"n": "21"})
    doc = c.get(_TABLE, doc_id)
    # Field reads are text extraction, so the int64 decimal string round-trips
    # through the cast into the int64 wire form.
    assert doc is not None and doc["echo"] == "21"


# ---------------------------------------------------------------------------
# Migrate interplay
# ---------------------------------------------------------------------------


def _migrate_client() -> InMemoryRtDbClient:
    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("first", t.string())
                .field("last", t.string())
                .field("status", t.string())
                .field("fullName", t.string())
                .field("tier", t.optional(t.string()))
                .index("by_fullName", ["fullName"])
                .computed(
                    "fullName",
                    ve.case(
                        [
                            {
                                "when": {"op": "eq", "field": "status", "value": "admin"},
                                "then": ve.concat(
                                    ve.field("first"), ve.literal("*"), ve.field("last")
                                ),
                            }
                        ],
                        ve.concat(ve.field("first"), ve.literal(" "), ve.field("last")),
                    ),
                )
                .computed("tier", ve.coalesce(ve.field("status"))),
            ),
        )
        .build()
    )
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 10_000, random=lambda: 0.5))
    c.push_schema(schema)
    return c


def test_migrate_rename_rewrites_field_refs_case_whens_and_keyed_entry() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    ada = _insert(c, {"first": "Ada", "last": "Lovelace", "status": "admin"})
    directives = Migration.builder().rename_field(_TABLE, "first", "givenName").build().directives
    result = c.migrate_schema(directives)

    table = result.schema_.tables[_TABLE]
    # Field refs (concat) and Case.when filter refs rewritten.
    assert "fullName" in table.computed
    dumped = table.computed["fullName"].model_dump(by_alias=True, mode="json")
    assert dumped == {
        "op": "case",
        "whens": [
            {
                "when": {"op": "eq", "field": "status", "value": "admin"},
                "then": {
                    "op": "concat",
                    "parts": [
                        {"op": "field", "field": "givenName"},
                        {"op": "literal", "value": "*"},
                        {"op": "field", "field": "last"},
                    ],
                },
            }
        ],
        "otherwise": {
            "op": "concat",
            "parts": [
                {"op": "field", "field": "givenName"},
                {"op": "literal", "value": " "},
                {"op": "field", "field": "last"},
            ],
        },
    }
    # The renamed table validates (post-fold _validate_computed passes) and the
    # stored doc's key was renamed with the computed value intact.
    assert result.applied is True
    doc = c.get(_TABLE, ada)
    assert doc is not None
    assert doc["givenName"] == "Ada"
    assert doc["fullName"] == "Ada*Lovelace"


def test_migrate_rename_moves_computed_key_onto_new_name() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    directives = (
        Migration.builder().rename_field(_TABLE, "fullName", "displayName").build().directives
    )
    result = c.migrate_schema(directives)
    table = result.schema_.tables[_TABLE]
    assert "displayName" in table.computed and "fullName" not in table.computed


def test_migrate_drop_field_on_referenced_input_rejected() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    directives = Migration.builder().drop_field(_TABLE, "first").build().directives
    with pytest.raises(RtDbError) as ei:
        c.migrate_schema(directives)
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert (
        "cannot drop field 'users.first': it is referenced by computed field"
        " 'users.fullName'" in ei.value.message
    )
    assert ei.value.message.endswith("drop the computed field first")
    # Nothing persisted: the schema still carries the entry.
    schema = c.to_schema_json()
    assert schema is not None and "fullName" in schema.tables[_TABLE].computed


def test_migrate_drop_computed_field_removes_its_entry() -> None:
    from par_rt_db import Migration

    c = _migrate_client()
    directives = Migration.builder().drop_field(_TABLE, "fullName").build().directives
    result = c.migrate_schema(directives)
    table = result.schema_.tables[_TABLE]
    assert "fullName" not in table.computed
    assert "tier" in table.computed  # siblings untouched


def test_migrate_change_type_revalidates_computed() -> None:
    from par_rt_db import Cast, Migration

    # A statically-kinded entry (`lower` → String) on a field a changeType
    # retypes to number must fail the POST-FOLD re-validation before anything
    # commits — the plan's `validate_computed` mirror. (A `case`/`coalesce`
    # entry would skip the static check; the corpus pins that too.) No rows
    # seeded, so the changeType applier itself succeeds.
    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("nick", t.string())
                .field("slug", t.string())
                .index("by_slug", ["slug"])
                .computed("slug", ve.lower(ve.trim(ve.field("nick"))))
            ),
        )
        .build()
    )
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 0, random=lambda: 0.5))
    c.push_schema(schema)
    directives = (
        Migration.builder()
        .change_type(_TABLE, "slug", {"type": "number"}, Cast.TO_NUMBER)
        .build()
        .directives
    )
    with pytest.raises(RtDbError) as ei:
        c.migrate_schema(directives)
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "produces a string" in ei.value.message
    # Nothing persisted — the installed schema still declares the string slug.
    installed = c.to_schema_json()
    assert installed is not None and installed.tables[_TABLE].fields["slug"].type == "string"
