"""Tests for the server-assigned ``autoIncrementField`` (FM-37) in the schema
DSL and the in-memory harness.

Mirrors ``server/tests/auto_increment_test.rs`` at engine level: push-time
validation (undeclared / non-int64 / ttl and updatedAt collisions), insert
authority (sequential assignment overwriting client-supplied values, the
stamp winning over a ``defaults`` entry), post-insert immutability (patch /
replace / upsert-update / patchByQuery rejections with round-trip-friendly
equal values), declaration-added-to-a-populated-table repositioning past the
stored max, and re-push leaving the counter untouched. The server-only
concurrency and snapshot-import tests have no in-memory equivalent (the
harness is single-threaded and has no snapshot replay); the counter's
non-transactional gap-on-rollback behavior IS pinned here because the harness
implements it deliberately (the counter sits outside the rollback snapshot,
like the server's ``nextval``).
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import TypeAdapter

from par_rt_db import Mutation
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.schema import Schema, TableDef, t
from par_rt_db.wire import FilterExpr

_TABLE = "tickets"


def _flt(expr: dict[str, Any]) -> Any:
    """Validate a raw filter dict into the ``FilterExpr`` union (the
    ``patch_by_query`` parameter type) — the same idiom as test_updated_at."""
    return TypeAdapter(FilterExpr).validate_python(expr)


def _counter_schema(
    table_extra: dict[str, Any] | None = None,
    fields: dict[str, Any] | None = None,
) -> Any:
    """``tickets`` table with an int64 ``num`` field and the
    ``autoIncrementField`` policy naming it (the server test's
    ``counter_schema``)."""
    table: dict[str, Any] = {
        "fields": fields or {"title": {"type": "string"}, "num": {"type": "int64"}},
        "indexes": [{"name": "by_title", "fields": ["title"]}],
        "autoIncrementField": "num",
    }
    if table_extra:
        table.update(table_extra)
    return Schema.model_validate({"tables": {_TABLE: table}})


def _plain_schema() -> Any:
    """The same table WITHOUT the counter declaration — for testing a
    declaration added to an already-populated table."""
    return Schema.model_validate(
        {
            "tables": {
                _TABLE: {
                    "fields": {"title": {"type": "string"}, "num": {"type": "int64"}},
                    "indexes": [{"name": "by_title", "fields": ["title"]}],
                }
            }
        }
    )


def _new_client(schema: Any) -> InMemoryRtDbClient:
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 10_000, random=lambda: 0.5))
    c.push_schema(schema)
    return c


def _insert(c: InMemoryRtDbClient, doc: dict[str, Any]) -> str:
    """Insert ``doc`` into the tickets table and return its id."""
    [res] = c.mutate(Mutation.builder().insert(_TABLE, doc).build())
    assert res is not None
    return str(res.model_dump()["id"])


def _get(c: InMemoryRtDbClient, doc_id: str) -> dict[str, Any]:
    doc = c.get(_TABLE, doc_id)
    assert doc is not None
    return doc


def _counter(c: InMemoryRtDbClient, doc_id: str) -> str:
    """The stored counter value — an int64 decimal string, end to end."""
    return _get(c, doc_id)["num"]


def _push_error(schema_json: dict[str, Any], phrase: str) -> RtDbError:
    """Validate ``schema_json`` and push it into a fresh client, asserting the
    push is rejected with ``SCHEMA_VIOLATION`` carrying ``phrase``."""
    schema = Schema.model_validate(schema_json)
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 0, random=lambda: 0.5))
    with pytest.raises(RtDbError) as ei:
        c.push_schema(schema)
    assert ei.value.code is ErrorCode.SCHEMA_VIOLATION
    assert phrase in ei.value.message, f"unexpected error: {ei.value.message}"
    return ei.value


# ---- wire contract ----


def test_auto_increment_field_builder_round_trips_and_omits_when_unset() -> None:
    """The builder's ``auto_increment_field`` lands on the wire as the camelCase
    key ``autoIncrementField``; a table without one omits the key entirely
    (server ``skip_serializing_if = "Option::is_none"``)."""
    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("title", t.string())
                .field("num", t.int64())
                .index("by_title", ["title"])
                .auto_increment_field("num")
            ),
        )
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    assert wire["tables"][_TABLE]["autoIncrementField"] == "num"

    # A TableDef parsed from the wire shape dumps identically.
    parsed = TableDef.model_validate(
        {"fields": {"num": {"type": "int64"}}, "autoIncrementField": "num"}
    )
    assert parsed.model_dump(by_alias=True)["autoIncrementField"] == "num"

    # Absent policy: the key never appears on the wire.
    bare = Schema.builder().table("t", lambda tb: tb.field("name", t.string())).build()
    bare_wire = json.loads(bare.model_dump_json(by_alias=True))["tables"]["t"]
    assert "autoIncrementField" not in bare_wire


# ---- push-time validation ----


def test_push_rejects_undeclared_auto_increment_field() -> None:
    bad = {
        "tables": {
            _TABLE: {
                "fields": {"title": {"type": "string"}, "num": {"type": "int64"}},
                "indexes": [{"name": "by_title", "fields": ["title"]}],
                "autoIncrementField": "nope",
            }
        }
    }
    _push_error(bad, "autoIncrementField 'nope' is not a declared field")


def test_push_rejects_non_int64_auto_increment_field() -> None:
    for num_type in (
        {"type": "number"},
        {"type": "string"},
        {"type": "optional", "inner": {"type": "int64"}},
    ):
        bad = {
            "tables": {
                _TABLE: {
                    "fields": {"title": {"type": "string"}, "num": num_type},
                    "indexes": [{"name": "by_title", "fields": ["title"]}],
                    "autoIncrementField": "num",
                }
            }
        }
        _push_error(bad, "autoIncrementField 'num' must be an int64 field")


def test_push_rejects_counter_colliding_with_ttl_or_updated_at() -> None:
    # The counter doubles as the ttl field (ttl validation requires a
    # single-field btree index on it first).
    ttl_bad = {
        "tables": {
            _TABLE: {
                "fields": {"title": {"type": "string"}, "num": {"type": "int64"}},
                "indexes": [
                    {"name": "by_title", "fields": ["title"]},
                    {"name": "by_num", "fields": ["num"]},
                ],
                "autoIncrementField": "num",
                "ttl": {"field": "num"},
            }
        }
    }
    _push_error(ttl_bad, "autoIncrementField 'num' must differ from ttl.field")

    at_bad = {
        "tables": {
            _TABLE: {
                "fields": {"title": {"type": "string"}, "num": {"type": "int64"}},
                "indexes": [{"name": "by_title", "fields": ["title"]}],
                "autoIncrementField": "num",
                "updatedAtField": "num",
            }
        }
    }
    _push_error(at_bad, "autoIncrementField 'num' must differ from updatedAtField")


# ---- insert authority ----


def test_insert_assigns_sequential_values_and_overwrites_client_value() -> None:
    c = _new_client(_counter_schema())

    # A client-supplied value (even a plausible one) is overwritten: the
    # first insert is 1 regardless.
    assert _counter(c, _insert(c, {"title": "A", "num": "999"})) == "1"
    assert _counter(c, _insert(c, {"title": "B"})) == "2"
    assert _counter(c, _insert(c, {"title": "C"})) == "3"


def test_stamp_wins_over_defaults_entry() -> None:
    schema = _counter_schema(table_extra={"defaults": {"num": "42"}})
    c = _new_client(schema)
    assert _counter(c, _insert(c, {"title": "A"})) == "1"


# ---- post-insert immutability ----


def test_patch_cannot_change_the_counter() -> None:
    c = _new_client(_counter_schema())
    doc_id = _insert(c, {"title": "A"})

    # Changing the value is rejected.
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"num": "99"}).build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "autoIncrementField 'num' cannot be changed" in ei.value.message

    # Round-tripping the same value is allowed.
    c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"num": "1"}).build())
    assert _counter(c, doc_id) == "1"


def test_replace_preserves_or_rejects_the_counter() -> None:
    c = _new_client(_counter_schema())
    doc_id = _insert(c, {"title": "A"})

    # A replace that omits the field keeps the stored value (it validates as
    # a complete document only because the engine fills it back in).
    c.mutate(Mutation.builder().replace(_TABLE, doc_id, {"title": "A2"}).build())
    doc = _get(c, doc_id)
    assert doc["num"] == "1"
    assert doc["title"] == "A2"

    # A replace that changes the value is rejected.
    with pytest.raises(RtDbError) as ei:
        c.mutate(Mutation.builder().replace(_TABLE, doc_id, {"title": "A3", "num": "5"}).build())
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "autoIncrementField 'num' cannot be changed" in ei.value.message

    # Round-tripping the stored value works.
    c.mutate(Mutation.builder().replace(_TABLE, doc_id, {"title": "A4", "num": "1"}).build())
    assert _counter(c, doc_id) == "1"


def test_upsert_insert_assigns_and_update_preserves() -> None:
    c = _new_client(_counter_schema())

    [res] = c.mutate(
        Mutation.builder().upsert(_TABLE, "by_title", ["A"], {"title": "A"}, {"title": "A"}).build()
    )
    assert res is not None
    doc_id = str(res.model_dump()["id"])
    assert res.model_dump()["inserted"] is True
    assert _counter(c, doc_id) == "1"

    # Update branch: a patch without the counter preserves it.
    [res] = c.mutate(
        Mutation.builder()
        .upsert(_TABLE, "by_title", ["A"], {"title": "never"}, {"title": "A2"})
        .build()
    )
    assert res is not None
    assert res.model_dump()["inserted"] is False
    assert _counter(c, doc_id) == "1"

    # Update branch: changing the counter is rejected.
    with pytest.raises(RtDbError) as ei:
        c.mutate(
            Mutation.builder()
            .upsert(_TABLE, "by_title", ["A2"], {"title": "never"}, {"num": "7"})
            .build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "autoIncrementField 'num' cannot be changed" in ei.value.message


def test_patch_by_query_cannot_change_the_counter() -> None:
    c = _new_client(_counter_schema())
    doc_id = _insert(c, {"title": "A"})

    with pytest.raises(RtDbError) as ei:
        c.mutate(
            Mutation.builder()
            .patch_by_query(
                _TABLE, _flt({"op": "eq", "field": "title", "value": "A"}), {"num": "50"}
            )
            .build()
        )
    assert ei.value.code is ErrorCode.BAD_REQUEST
    assert "autoIncrementField 'num' cannot be changed" in ei.value.message
    assert _counter(c, doc_id) == "1"


# ---- counter positioning ----


def test_declaration_added_to_populated_table_repositions_past_max() -> None:
    # v1: plain int64 field, client-supplied values 1..=5 (no counter yet).
    c = _new_client(_plain_schema())
    for i in range(1, 6):
        _insert(c, {"title": f"t{i}", "num": str(i)})

    # v2: same schema plus the declaration — additive push.
    c.push_schema(_counter_schema())
    assert _counter(c, _insert(c, {"title": "new"})) == "6"


def test_re_push_does_not_disturb_the_counter() -> None:
    c = _new_client(_counter_schema())
    _insert(c, {"title": "A"})
    _insert(c, {"title": "B"})

    # An unrelated additive push (new field) must not reposition anything.
    evolved = _counter_schema(
        fields={
            "title": {"type": "string"},
            "num": {"type": "int64"},
            "owner": {"type": "optional", "inner": {"type": "string"}},
        }
    )
    c.push_schema(evolved)
    assert _counter(c, _insert(c, {"title": "C"})) == "3"


def test_rolled_back_txn_leaves_a_gap() -> None:
    """The counter sits outside the rollback snapshot (the server's ``nextval``
    is non-transactional): a rolled-back txn still consumes its number, so the
    sequence is monotonic but not gap-free."""
    c = _new_client(_counter_schema())
    _insert(c, {"title": "A"})  # 1

    with pytest.raises(RtDbError):
        c.mutate(
            Mutation.builder()
            .insert(_TABLE, {"title": "B"})  # consumes 2 …
            .patch(_TABLE, "missing", {"title": "X"})  # … then the txn fails
            .build()
        )
    assert _counter(c, _insert(c, {"title": "C"})) == "3"
