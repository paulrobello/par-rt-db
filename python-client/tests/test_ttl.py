"""Tests for TTL (time-to-live) in the in-memory harness.

Mirrors the ``ts-client`` / ``rust-client`` TTL tests: a table that declares a
``ttl`` policy stamps ``defaultDurationMs`` onto inserts that omit the TTL field,
and :meth:`InMemoryRtDbClient.tick` reaps docs whose TTL field is in the past.
Also covers the wire contract (camelCase alias, ``None`` omission) and that
toggling ``ttl`` on a table is a non-destructive schema change.
"""

from __future__ import annotations

from typing import Any

from par_rt_db import Mutation, TableQuery
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.schema import Schema, TableDef

_TABLE = "sessions"


def _ttl_schema() -> Any:
    """``sessions`` table with a required numeric ``expiresAt`` TTL field and a
    1000ms default duration."""
    return Schema.model_validate(
        {
            "tables": {
                _TABLE: {
                    "fields": {"expiresAt": {"type": "number"}},
                    "indexes": [{"name": "by_expires", "fields": ["expiresAt"]}],
                    "ttl": {"field": "expiresAt", "defaultDurationMs": 1000},
                }
            }
        }
    )


def _new_client() -> InMemoryRtDbClient:
    # Frozen clock at 10_000ms so the stamped default expiry is exactly 11_000.
    clock = [10_000]

    def now() -> int:
        return clock[0]

    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.5))
    c.push_schema(_ttl_schema())
    return c


def _insert(c: InMemoryRtDbClient, doc: dict[str, Any]) -> str:
    """Insert ``doc`` into the sessions table and return its id (narrowing the
    ``StepResult | None`` union via ``model_dump`` — the union also carries
    ``patchByQuery``/``deleteByQuery`` shapes with no ``id``)."""
    [res] = c.mutate(Mutation.builder().insert(_TABLE, doc).build())
    assert res is not None
    return str(res.model_dump()["id"])


def test_ttl_serializes_with_camel_alias_and_omits_none() -> None:
    """Wire contract: a present ``ttl`` serializes its keys as ``field`` /
    ``defaultDurationMs`` (camelCase), and a ``TableDef`` without ``ttl`` omits
    the key entirely."""
    wire = _ttl_schema().tables[_TABLE].model_dump(by_alias=True)
    assert wire["ttl"] == {"field": "expiresAt", "defaultDurationMs": 1000}

    # A TtlDef without a default omits defaultDurationMs on the wire.
    no_default = TableDef.model_validate(
        {"fields": {"expiresAt": {"type": "number"}}, "ttl": {"field": "expiresAt"}}
    ).model_dump(by_alias=True)
    assert no_default["ttl"] == {"field": "expiresAt"}

    # A TableDef that does not declare ttl never emits the key.
    bare = TableDef.model_validate({"fields": {"a": {"type": "string"}}}).model_dump(by_alias=True)
    assert "ttl" not in bare


def test_insert_stamps_default_duration_when_field_absent() -> None:
    c = _new_client()  # clock frozen at 10_000
    doc_id = _insert(c, {})
    doc = c.get(_TABLE, doc_id)
    assert doc is not None
    # Default stamps now + defaultDurationMs (1000) because the caller omitted it.
    assert doc["expiresAt"] == 10_000 + 1000


def test_insert_respects_explicit_caller_value() -> None:
    c = _new_client()  # clock frozen at 10_000; the default would have been 11_000
    explicit = 99_999
    doc_id = _insert(c, {"expiresAt": explicit})
    doc = c.get(_TABLE, doc_id)
    assert doc is not None
    assert doc["expiresAt"] == explicit  # caller wins; default NOT applied


def test_tick_reaps_default_stamped_doc_past_expiry() -> None:
    c = _new_client()  # stamped expiry = 11_000
    doc_id = _insert(c, {})
    assert c.get(_TABLE, doc_id) is not None
    # Equal to expiry is NOT past (`< now`, not `<=`); the doc survives.
    c.tick(now_ms=11_000)
    assert c.get(_TABLE, doc_id) is not None
    # One ms past expiry -> reaped.
    c.tick(now_ms=11_001)
    assert c.get(_TABLE, doc_id) is None


def test_tick_keeps_unexpired_doc() -> None:
    c = _new_client()
    doc_id = _insert(c, {})
    c.tick(now_ms=10_500)  # before the stamped 11_000 expiry
    assert c.get(_TABLE, doc_id) is not None


def test_tick_reaps_explicit_caller_value() -> None:
    c = _new_client()
    doc_id = _insert(c, {"expiresAt": 10_500})
    c.tick(now_ms=10_500)  # equal is not past
    assert c.get(_TABLE, doc_id) is not None
    c.tick(now_ms=10_501)
    assert c.get(_TABLE, doc_id) is None


def test_absent_ttl_field_is_left_alone() -> None:
    """A doc that omits the TTL field (optional field, no default declared) is
    never reaped - the reaper only removes docs whose field is a number < now."""
    schema = Schema.model_validate(
        {
            "tables": {
                _TABLE: {
                    "fields": {"expiresAt": {"type": "optional", "inner": {"type": "number"}}},
                    "ttl": {"field": "expiresAt"},  # no defaultDurationMs
                }
            }
        }
    )
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 10_000, random=lambda: 0.5))
    c.push_schema(schema)
    doc_id = _insert(c, {})
    # No default -> expiresAt stays absent; ticking far into the future keeps it.
    c.tick(now_ms=1_000_000)
    assert c.get(_TABLE, doc_id) is not None


def test_non_numeric_ttl_value_is_left_alone() -> None:
    """A non-numeric TTL value is never reaped (the reaper's isinstance guard)."""
    schema = Schema.model_validate(
        {
            "tables": {
                _TABLE: {
                    "fields": {"expiresAt": {"type": "any"}},
                    "ttl": {"field": "expiresAt"},
                }
            }
        }
    )
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 10_000, random=lambda: 0.5))
    c.push_schema(schema)
    doc_id = _insert(c, {"expiresAt": "not-a-number"})
    c.tick(now_ms=1_000_000)
    assert c.get(_TABLE, doc_id) is not None


def test_tick_reap_fires_subscriptions() -> None:
    """Reaping through ``tick`` notifies reactive subscribers (the expiry is
    observed as a delete on the affected table)."""
    c = _new_client()
    _insert(c, {})
    seen: list[Any] = []
    handle = c.subscribe(TableQuery(_TABLE).build(), lambda v: seen.append(v))
    try:
        assert len(seen[-1]) == 1  # initial fire: one row
        c.tick(now_ms=11_001)  # reap the expired doc
        assert seen[-1] == []  # post-reap push: empty result
    finally:
        handle.unsubscribe()


def test_ttl_toggle_is_non_destructive() -> None:
    """Adding then removing a ``ttl`` policy on an existing table must not raise -
    it is a behavior toggle, not a structural (field/index) change. Mirrors
    ``ddl::detect_destructive_changes``, which inspects fields/indexes only."""
    c = _new_client()
    without_ttl = Schema.model_validate(
        {
            "tables": {
                _TABLE: {
                    "fields": {"expiresAt": {"type": "number"}},
                    "indexes": [{"name": "by_expires", "fields": ["expiresAt"]}],
                }
            }
        }
    )
    c.push_schema(without_ttl)  # removing ttl - non-destructive
    c.push_schema(_ttl_schema())  # re-adding ttl - also non-destructive


def test_tick_without_ttl_does_nothing() -> None:
    """A table without a ``ttl`` policy is untouched by ``tick`` reaping."""
    schema = Schema.model_validate(
        {
            "tables": {
                _TABLE: {
                    "fields": {"expiresAt": {"type": "number"}},
                    "indexes": [{"name": "by_expires", "fields": ["expiresAt"]}],
                }
            }
        }
    )  # no ttl
    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=lambda: 10_000, random=lambda: 0.5))
    c.push_schema(schema)
    doc_id = _insert(c, {"expiresAt": 1})  # would be "expired" if a ttl existed
    c.tick(now_ms=1_000_000)
    assert c.get(_TABLE, doc_id) is not None
