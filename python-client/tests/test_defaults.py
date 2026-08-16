"""Tests for field-level default values (FM-32) in the schema DSL and the
in-memory harness.

Mirrors the server's semantics (``schema.rs::TableDef.defaults`` +
``txn.rs::apply_defaults``): defaults stamp a NEW document that omits the key —
insert, replace, and upsert-insert only; patch (and upsert-update /
patchByQuery) never re-apply, so clearing an optional field stays cleared. A
ttl ``defaultDurationMs`` on the same field wins over a defaults entry (the ttl
stamp runs first). Also covers the wire contract: the ``defaults`` key is
present when non-empty and omitted entirely when empty/absent.
"""

from __future__ import annotations

import json
from typing import Any

from par_rt_db import Mutation
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.schema import Schema, TableDef, t

_TABLE = "tasks"


def _defaults_schema() -> Any:
    """``tasks`` table: required ``title``, optional ``status``/``priority``/
    ``owner`` (indexed for the upsert test), with defaults on the first two."""
    return Schema.model_validate(
        {
            "tables": {
                _TABLE: {
                    "fields": {
                        "title": {"type": "string"},
                        "status": {"type": "optional", "inner": {"type": "string"}},
                        "priority": {"type": "optional", "inner": {"type": "number"}},
                        "owner": {"type": "optional", "inner": {"type": "string"}},
                    },
                    "indexes": [{"name": "by_owner", "fields": ["owner"]}],
                    "defaults": {"status": "backlog", "priority": 0},
                }
            }
        }
    )


def _new_client(schema: Any) -> InMemoryRtDbClient:
    # Frozen clock at 10_000ms so a ttl-default stamp is exactly now + duration.
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 10_000, random=lambda: 0.5))
    c.push_schema(schema)
    return c


def _insert(c: InMemoryRtDbClient, doc: dict[str, Any]) -> str:
    """Insert ``doc`` into the tasks table and return its id (narrowing the
    ``StepResult | None`` union via ``model_dump``)."""
    [res] = c.mutate(Mutation.builder().insert(_TABLE, doc).build())
    assert res is not None
    return str(res.model_dump()["id"])


def _get(c: InMemoryRtDbClient, doc_id: str) -> dict[str, Any]:
    doc = c.get(_TABLE, doc_id)
    assert doc is not None
    return doc


def test_defaults_builder_round_trips_and_empty_omits_key() -> None:
    """Wire contract: a non-empty ``defaults`` map round-trips as the wire key
    ``defaults``; a table without one (or with an empty map) omits the key
    entirely (server ``BTreeMap::is_empty`` skip rule)."""
    schema = (
        Schema.builder()
        .table(
            "tasks",
            lambda tb: (
                tb.field("title", t.string())
                .field("status", t.optional(t.string()))
                .field("priority", t.optional(t.number()))
                .defaults({"status": "backlog", "priority": 0})
            ),
        )
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    assert wire["tables"]["tasks"]["defaults"] == {"status": "backlog", "priority": 0}

    # A TableDef parsed from the wire shape dumps identically.
    parsed = TableDef.model_validate(
        {
            "fields": {"status": {"type": "optional", "inner": {"type": "string"}}},
            "defaults": {"status": "backlog"},
        }
    )
    assert parsed.model_dump(by_alias=True)["defaults"] == {"status": "backlog"}

    # Absent defaults: the key never appears on the wire.
    bare = Schema.builder().table("t", lambda tb: tb.field("name", t.string())).build()
    assert "defaults" not in json.loads(bare.model_dump_json(by_alias=True))["tables"]["t"]

    # An explicitly empty map is omitted too.
    empty = (
        Schema.builder().table("t", lambda tb: tb.field("name", t.string()).defaults({})).build()
    )
    assert "defaults" not in json.loads(empty.model_dump_json(by_alias=True))["tables"]["t"]


def test_insert_applies_defaults_for_omitted_keys() -> None:
    c = _new_client(_defaults_schema())
    doc = _get(c, _insert(c, {"title": "write tests"}))
    assert doc["status"] == "backlog"
    assert doc["priority"] == 0


def test_client_value_wins_over_default() -> None:
    c = _new_client(_defaults_schema())
    doc = _get(c, _insert(c, {"title": "ship", "status": "done", "priority": 5}))
    assert doc["status"] == "done"
    assert doc["priority"] == 5


def test_patch_does_not_reapply_after_clearing_optional() -> None:
    """Patching an optional field to null clears it, and a later patch must NOT
    re-stamp the default — patch never applies defaults."""
    c = _new_client(_defaults_schema())
    doc_id = _insert(c, {"title": "a"})

    c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"status": None}).build())
    doc = _get(c, doc_id)
    assert "status" not in doc

    # A second, unrelated patch still leaves the cleared field absent.
    c.mutate(Mutation.builder().patch(_TABLE, doc_id, {"priority": 2}).build())
    doc = _get(c, doc_id)
    assert "status" not in doc
    assert doc["priority"] == 2


def test_replace_reapplies_defaults() -> None:
    """Replace writes a whole NEW document: an omitted key gets its default
    again (unlike patch)."""
    c = _new_client(_defaults_schema())
    doc_id = _insert(c, {"title": "a", "status": "done"})
    c.mutate(Mutation.builder().replace(_TABLE, doc_id, {"title": "b"}).build())
    doc = _get(c, doc_id)
    assert doc["title"] == "b"
    assert doc["status"] == "backlog"
    assert doc["priority"] == 0


def test_upsert_insert_applies_and_update_does_not() -> None:
    c = _new_client(_defaults_schema())

    # No match: the insert branch stamps defaults onto the inserted doc.
    [res] = c.mutate(
        Mutation.builder()
        .upsert(_TABLE, "by_owner", ["u1"], {"title": "first", "owner": "u1"}, {"priority": 9})
        .build()
    )
    assert res is not None
    first = res.model_dump()
    assert first["inserted"] is True
    doc = _get(c, str(first["id"]))
    assert doc["status"] == "backlog"
    assert doc["priority"] == 0

    # Match: the patch branch applies the patch only — no defaults re-stamp,
    # so clearing the defaulted field stays cleared.
    [res] = c.mutate(
        Mutation.builder()
        .upsert(_TABLE, "by_owner", ["u1"], {"title": "never"}, {"status": None})
        .build()
    )
    assert res is not None
    again = res.model_dump()
    assert again["inserted"] is False
    assert again["id"] == first["id"]
    doc = _get(c, str(first["id"]))
    assert doc["title"] == "first"
    assert "status" not in doc


def test_ttl_default_wins_over_defaults_entry_on_same_field() -> None:
    """The ttl stamp runs before apply_defaults, so a ttl defaultDurationMs on
    the same field wins: the key is present when defaults run and is skipped."""
    schema = Schema.model_validate(
        {
            "tables": {
                "sessions": {
                    "fields": {"expiresAt": {"type": "number"}},
                    "ttl": {"field": "expiresAt", "defaultDurationMs": 1000},
                    "defaults": {"expiresAt": 999_999},
                }
            }
        }
    )
    c = _new_client(schema)  # clock frozen at 10_000
    [res] = c.mutate(Mutation.builder().insert("sessions", {}).build())
    assert res is not None
    doc = c.get("sessions", str(res.model_dump()["id"]))
    assert doc is not None
    assert doc["expiresAt"] == 11_000
