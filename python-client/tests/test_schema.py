"""Tests for ``par_rt_db.schema``: FieldType (15 variants), IndexDef,
``VectorIndexSpec``, ``TableDef`` (+ ``ownerField``), ``SchemaDef``, and the
``Schema``/``TableBuilder`` fluent APIs plus the ``t`` field constructors.

Wire shapes mirror:
  - server/src/schema.rs       (FieldType enum, IndexDef, VectorIndexSpec, TableDef)
  - rust-client/src/schema.rs  (FieldType, builders)
  - ts-client/src/schema.ts    (t.* constructors)
"""

import json

import pytest
from pydantic import ValidationError

from par_rt_db.schema import IndexDef, Schema, t


def test_scalar_and_compound_fields_round_trip():
    schema = (
        Schema.builder()
        .table(
            "players",
            lambda tb: (
                tb.field("email", t.string())
                .field("age", t.number())
                .field("alive", t.boolean())
                .field("nick", t.null())
                .field("ref", t.id("players"))
                .field("role", t.literal("admin"))
                .field("tags", t.array(t.string()))
                .field("meta", t.optional(t.object({"x": t.number()})))
                .field("mix", t.union([t.string(), t.number()]))
                .field("kv", t.record(t.number()))
                .field("raw", t.any())
                .field("b", t.bytes())
                .field("big", t.int64())
                .field("emb", t.vector(8))
            ),
        )
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    assert set(wire["tables"]["players"]["fields"].keys()) == {
        "email",
        "age",
        "alive",
        "nick",
        "ref",
        "role",
        "tags",
        "meta",
        "mix",
        "kv",
        "raw",
        "b",
        "big",
        "emb",
    }
    assert wire["tables"]["players"]["fields"]["email"] == {"type": "string"}
    assert wire["tables"]["players"]["fields"]["ref"] == {"type": "id", "table": "players"}
    assert wire["tables"]["players"]["fields"]["emb"] == {"type": "vector", "dimensions": 8}
    assert wire["tables"]["players"]["fields"]["big"] == {"type": "int64"}


def test_indexes_search_vector_owner_field():
    schema = (
        Schema.builder()
        .table(
            "boxes",
            lambda tb: (
                tb.field("status", t.string())
                .field("owner_id", t.id("players"))
                .field("embedding", t.vector(4))
                .index("by_status", ["status"])
                .search_index("text_idx", ["status"])
                .vector_index("emb_idx", "embedding", 4, filter_fields=["owner_id"])
                .owner_field("owner_id")
            ),
        )
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    tbl = wire["tables"]["boxes"]
    assert tbl["indexes"][0] == {"name": "by_status", "fields": ["status"]}
    assert tbl["indexes"][1] == {"name": "text_idx", "fields": ["status"], "search": True}
    assert tbl["indexes"][2] == {
        "name": "emb_idx",
        "fields": ["embedding"],
        "vector": {"dimensions": 4, "filterFields": ["owner_id"]},
    }
    assert tbl["ownerField"] == "owner_id"


def test_plain_index_omits_search_and_vector_keys():
    """A btree index must serialize as ``{name, fields}`` only — ``search`` and
    ``vector`` are omitted on the wire when absent (mirrors the server's
    ``skip_serializing_if`` rules)."""
    schema = (
        Schema.builder()
        .table("t", lambda tb: tb.field("name", t.string()).index("by_name", ["name"]))
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    idx = wire["tables"]["t"]["indexes"][0]
    assert idx == {"name": "by_name", "fields": ["name"]}
    assert "search" not in idx
    assert "vector" not in idx


def test_indexdef_with_search_false_omits_search_key():
    """The server uses ``skip_serializing_if = "is_false"`` for ``IndexDef.search``
    (a ``bool``, not ``Option<bool>``), so an ``IndexDef`` carrying
    ``search=False`` must omit ``search`` from the wire — not emit ``"search":
    false``. Hand-construct the model to bypass the builder (which only ever
    sets ``search=True``) and prove the falsy-drop branch directly."""
    idx = IndexDef.model_validate({"name": "by_name", "fields": ["name"], "search": False})
    wire = json.loads(idx.model_dump_json(by_alias=True))
    assert wire == {"name": "by_name", "fields": ["name"]}
    assert "search" not in wire


def test_table_without_owner_field_omits_ownerField_key():
    schema = Schema.builder().table("t", lambda tb: tb.field("name", t.string())).build()
    wire = json.loads(schema.model_dump_json(by_alias=True))
    assert "ownerField" not in wire["tables"]["t"]


def test_vector_index_without_filter_fields_omits_filterFields():
    """``VectorIndexSpec.filterFields`` is omitted on the wire when empty
    (mirrors server's ``Vec::is_empty`` skip rule)."""
    schema = (
        Schema.builder()
        .table(
            "docs",
            lambda tb: tb.field("embedding", t.vector(8)).vector_index("by_emb", "embedding", 8),
        )
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    vec = wire["tables"]["docs"]["indexes"][0]["vector"]
    assert vec == {"dimensions": 8}
    assert "filterFields" not in vec


def test_field_type_rejects_unknown():
    with pytest.raises(ValidationError):
        t._validate({"type": "bogus"})


def test_field_type_rejects_unknown_nested_field_key():
    """extra='forbid' on every variant: an unknown key on a valid tag fails."""
    with pytest.raises(ValidationError):
        t._validate({"type": "string", "bogus": 1})


def test_schema_rejects_unknown_top_keys():
    with pytest.raises(ValidationError):
        Schema.model_validate({"tables": {}, "bogus": 1})


def test_nested_compound_field_round_trips():
    """Optional<Union<[string, number]>> survives a full wire round-trip."""
    schema = (
        Schema.builder()
        .table("things", lambda tb: tb.field("val", t.optional(t.union([t.string(), t.number()]))))
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    assert wire["tables"]["things"]["fields"]["val"] == {
        "type": "optional",
        "inner": {"type": "union", "variants": [{"type": "string"}, {"type": "number"}]},
    }


def test_schema_round_trips_through_json_parse():
    """Build → dump_json → parse → model_validate is identity (accepts own output)."""
    schema = (
        Schema.builder()
        .table(
            "players",
            lambda tb: (
                tb.field("email", t.string())
                .field("ref", t.id("players"))
                .field("emb", t.vector(8))
                .index("by_email", ["email"])
                .owner_field("email")
            ),
        )
        .build()
    )
    dumped = schema.model_dump_json(by_alias=True)
    reparsed = Schema.model_validate(json.loads(dumped))
    assert reparsed.model_dump_json(by_alias=True) == dumped
