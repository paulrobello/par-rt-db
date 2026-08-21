"""Tests for the server-stamped ``updatedAtField`` (FM-36) in the schema DSL
and the in-memory harness.

Mirrors ``server/tests/updated_at_test.rs`` at engine level: push-time
validation (undeclared / non-numeric / ttl-collision) and stamp semantics on
every version-bumping write path — insert, patch, replace, upsert (both
branches), patchByQuery, and cascade setNull — each overwriting a
client-supplied value. The stamp wins over a ``defaults`` entry on the same
field (same authority family as the ttl ``defaultDurationMs`` stamp). The
injectable clock makes every restamp assertion exact (no sleeps): the frozen
clock pins the first stamp and a ``clock[0] += 1`` tick proves strict
inequality on the restamp.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import TypeAdapter

from par_rt_db import Mutation
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.schema import IndexDef, Schema, TableDef, t
from par_rt_db.wire import FilterExpr

_TABLE = "tasks"


def _flt(expr: dict[str, Any]) -> Any:
    """Validate a raw filter dict into the ``FilterExpr`` union (the
    ``patch_by_query`` parameter type) — the same idiom as test_in_memory."""
    return TypeAdapter(FilterExpr).validate_python(expr)


def _updated_at_schema(
    updated_at_type: str = "number",
    *,
    table_extra: dict[str, Any] | None = None,
) -> Any:
    """``tasks`` table with a declared ``updatedAt`` field of
    ``updated_at_type`` and the ``updatedAtField`` policy naming it."""
    table: dict[str, Any] = {
        "fields": {"title": {"type": "string"}, "updatedAt": {"type": updated_at_type}},
        "indexes": [{"name": "by_title", "fields": ["title"]}],
        "updatedAtField": "updatedAt",
    }
    if table_extra:
        table.update(table_extra)
    return Schema.model_validate({"tables": {_TABLE: table}})


def _new_client(schema: Any) -> tuple[InMemoryRtDbClient, list[int]]:
    """Client with a mutable frozen clock (10_000ms); ``clock[0] += 1`` is the
    deterministic stand-in for the server test's ``tick()`` sleep."""
    clock = [10_000]
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: clock[0], random=lambda: 0.5))
    c.push_schema(schema)
    return c, clock


def _insert(c: InMemoryRtDbClient, doc: dict[str, Any]) -> str:
    """Insert ``doc`` into the tasks table and return its id."""
    [res] = c.mutate(Mutation.builder().insert(_TABLE, doc).build())
    assert res is not None
    return str(res.model_dump()["id"])


def _get(c: InMemoryRtDbClient, doc_id: str) -> dict[str, Any]:
    doc = c.get(_TABLE, doc_id)
    assert doc is not None
    return doc


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


def test_updated_at_field_builder_round_trips_and_omits_when_unset() -> None:
    """The builder's ``updated_at_field`` lands on the wire as the camelCase key
    ``updatedAtField``; a table without one omits the key entirely (server
    ``skip_serializing_if = "Option::is_none"``)."""
    schema = (
        Schema.builder()
        .table(
            _TABLE,
            lambda tb: (
                tb.field("title", t.string())
                .field("updatedAt", t.number())
                .index("by_title", ["title"])
                .updated_at_field("updatedAt")
            ),
        )
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    assert wire["tables"][_TABLE]["updatedAtField"] == "updatedAt"

    # A TableDef parsed from the wire shape dumps identically.
    parsed = TableDef.model_validate(
        {
            "fields": {"updatedAt": {"type": "number"}},
            "updatedAtField": "updatedAt",
        }
    )
    assert parsed.model_dump(by_alias=True)["updatedAtField"] == "updatedAt"

    # Absent policy: the key never appears on the wire.
    bare = Schema.builder().table("t", lambda tb: tb.field("name", t.string())).build()
    assert "updatedAtField" not in json.loads(bare.model_dump_json(by_alias=True))["tables"]["t"]


# ---- push-time validation ----


def test_push_rejects_undeclared_updated_at_field() -> None:
    bad = {
        "tables": {
            _TABLE: {
                "fields": {"title": {"type": "string"}, "updatedAt": {"type": "number"}},
                "indexes": [{"name": "by_title", "fields": ["title"]}],
                "updatedAtField": "nope",
            }
        }
    }
    _push_error(bad, "updatedAtField 'nope' is not a declared field")


def test_push_rejects_non_numeric_updated_at_field() -> None:
    bad = {
        "tables": {
            _TABLE: {
                "fields": {"title": {"type": "string"}, "updatedAt": {"type": "string"}},
                "indexes": [{"name": "by_title", "fields": ["title"]}],
                "updatedAtField": "updatedAt",
            }
        }
    }
    _push_error(bad, "updatedAtField 'updatedAt' must be a number or bigint field")


def test_push_rejects_updated_at_field_matching_ttl_field() -> None:
    """Both stamps write unconditionally, so a field shared with ``ttl.field``
    would silently drop the expiry — rejected at push time."""
    bad = {
        "tables": {
            "sessions": {
                "fields": {"token": {"type": "string"}, "expiresAt": {"type": "number"}},
                "indexes": [
                    {"name": "by_token", "fields": ["token"]},
                    {"name": "by_expiresAt", "fields": ["expiresAt"]},
                ],
                "ttl": {"field": "expiresAt"},
                "updatedAtField": "expiresAt",
            }
        }
    }
    _push_error(bad, "must differ from ttl.field")


# ---- stamp semantics: number field ----


def test_insert_stamps_and_overwrites_client_value() -> None:
    c, _clock = _new_client(_updated_at_schema())
    doc = _get(c, _insert(c, {"title": "A", "updatedAt": 123}))
    # The frozen clock pins the stamp exactly — the client's 123 is gone.
    assert doc["updatedAt"] == 10_000
    assert doc["title"] == "A"


def test_insert_stamps_int64_field_as_decimal_string() -> None:
    """int64 fields hold decimal strings end to end (the wire convention), so
    the stamp is ``str(now_ms)`` — also pins that the field is indexable."""
    schema = _updated_at_schema("int64")
    # Mirror the server test: add a typed index on updatedAt.
    schema.tables[_TABLE].indexes.append(
        IndexDef.model_validate({"name": "by_updatedAt", "fields": ["updatedAt"]})
    )
    c, _clock = _new_client(schema)
    doc = _get(c, _insert(c, {"title": "A"}))
    assert doc["updatedAt"] == "10000"
    assert int(doc["updatedAt"]) == 10_000


def test_patch_restamps_and_overwrites_client_value() -> None:
    c, clock = _new_client(_updated_at_schema())
    doc_id = _insert(c, {"title": "A"})
    first = _get(c, doc_id)["updatedAt"]
    clock[0] += 1  # deterministic tick

    c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"title": "B", "updatedAt": 1}).build())
    doc = _get(c, doc_id)
    assert doc["updatedAt"] == 10_001
    assert doc["updatedAt"] > first
    assert doc["title"] == "B"


def test_patch_that_omits_the_field_still_restamps() -> None:
    """The stamp joins the patch fields before the merge, so a patch that never
    mentions the field cannot preserve a stale value."""
    c, clock = _new_client(_updated_at_schema())
    doc_id = _insert(c, {"title": "A"})
    clock[0] += 1

    c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"title": "B"}).build())
    doc = _get(c, doc_id)
    assert doc["updatedAt"] == 10_001


def test_replace_restamps() -> None:
    c, clock = _new_client(_updated_at_schema())
    doc_id = _insert(c, {"title": "A"})
    first = _get(c, doc_id)["updatedAt"]
    clock[0] += 1

    c.mutate(Mutation.builder().replace(_TABLE, doc_id, {"title": "A2", "updatedAt": 7}).build())
    second = _get(c, doc_id)["updatedAt"]
    assert second == 10_001
    assert second > first


def test_upsert_insert_stamps_and_update_restamps() -> None:
    c, clock = _new_client(_updated_at_schema())

    [res] = c.mutate(
        Mutation.builder()
        .upsert(_TABLE, "by_title", ["A"], {"title": "A", "updatedAt": 9}, {})
        .build()
    )
    assert res is not None
    first = _get(c, str(res.model_dump()["id"]))["updatedAt"]
    assert first == 10_000  # insert branch stamps, overwriting the client's 9
    clock[0] += 1

    [res] = c.mutate(
        Mutation.builder()
        .upsert(_TABLE, "by_title", ["A"], {"title": "never"}, {"title": "A3", "updatedAt": 5})
        .build()
    )
    assert res is not None
    doc_id = str(res.model_dump()["id"])
    second = _get(c, doc_id)["updatedAt"]
    assert second == 10_001
    assert second > first


def test_patch_by_query_restamps() -> None:
    c, clock = _new_client(_updated_at_schema())
    doc_id = _insert(c, {"title": "A"})
    first = _get(c, doc_id)["updatedAt"]
    clock[0] += 1

    c.mutate(
        Mutation.builder()
        .patch_by_query(
            _TABLE, _flt({"op": "eq", "field": "title", "value": "A"}), {"updatedAt": 3}
        )
        .build()
    )
    second = _get(c, doc_id)["updatedAt"]
    assert second == 10_001
    assert second > first


def test_cascade_set_null_restamps_child() -> None:
    """Cascade setNull is a version-bumping write on the CHILD, so the child
    table's own ``updatedAtField`` restamps while the reference is nulled."""
    schema = Schema.model_validate(
        {
            "tables": {
                "parents": {
                    "fields": {"name": {"type": "string"}},
                    "indexes": [{"name": "by_name", "fields": ["name"]}],
                },
                "children": {
                    "fields": {
                        "parentId": {
                            "type": "optional",
                            "inner": {"type": "id", "table": "parents", "onDelete": "setNull"},
                        },
                        "title": {"type": "string"},
                        "updatedAt": {"type": "number"},
                    },
                    "indexes": [{"name": "by_parentId", "fields": ["parentId"]}],
                    "updatedAtField": "updatedAt",
                },
            }
        }
    )
    c, clock = _new_client(schema)

    [res] = c.mutate(Mutation.builder().insert("parents", {"name": "P"}).build())
    assert res is not None
    parent_id = str(res.model_dump()["id"])
    [res] = c.mutate(
        Mutation.builder().insert("children", {"parentId": parent_id, "title": "C"}).build()
    )
    assert res is not None
    child_id = str(res.model_dump()["id"])
    child = c.get("children", child_id)
    assert child is not None
    first = child["updatedAt"]
    clock[0] += 1

    c.mutate(Mutation.builder().delete("parents", parent_id).build())
    child = c.get("children", child_id)
    assert child is not None
    assert "parentId" not in child, "setNull removed the ref"
    assert child["updatedAt"] == 10_001
    assert child["updatedAt"] > first


def test_stamp_wins_over_defaults_entry() -> None:
    """The stamp runs before ``_apply_defaults`` on insert (server
    ``step_insert`` order), so a ``defaults`` entry on the same field loses."""
    schema = _updated_at_schema(table_extra={"defaults": {"updatedAt": 12345}})
    c, _clock = _new_client(schema)
    doc = _get(c, _insert(c, {"title": "A"}))
    assert doc["updatedAt"] == 10_000
    assert doc["updatedAt"] != 12345


def test_toggle_is_non_destructive_and_unstamped_table_never_stamps() -> None:
    """Adding then removing the policy is a non-destructive push (like ``ttl``),
    and once removed the field is ordinary — a client value survives."""
    c, _clock = _new_client(_updated_at_schema())
    without = Schema.model_validate(
        {
            "tables": {
                _TABLE: {
                    "fields": {"title": {"type": "string"}, "updatedAt": {"type": "number"}},
                    "indexes": [{"name": "by_title", "fields": ["title"]}],
                }
            }
        }
    )
    c.push_schema(without)  # removing the policy — non-destructive
    c.push_schema(_updated_at_schema())  # re-adding — also non-destructive

    # And with the policy absent, an insert keeps the client's value verbatim.
    c2, _ = _new_client(without)
    doc = _get(c2, _insert(c2, {"title": "A", "updatedAt": 555}))
    assert doc["updatedAt"] == 555
