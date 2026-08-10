"""ARC-109 — the public package surface must export the schedule-when variants,
``ScheduleWhen``, ``Schema``, and ``InMemoryRtDbClient`` from the package root.

ts-client (``index.ts``) and rust-client (``lib.rs``) both export
``ScheduleWhen``; the python client was the lone hold-out that required reaching
into the underscore-private ``par_rt_db.wire`` internals. These names are needed
to call the public ``schedule()`` methods (http / aio / ws) and to use the
in-memory harness without a sub-module import.
"""

from __future__ import annotations

import par_rt_db
from par_rt_db import wire
from par_rt_db.in_memory import InMemoryRtDbClient as _InMemoryRtDbClient
from par_rt_db.schema import Schema as _Schema


def test_schedule_when_union_exported() -> None:
    # ``ScheduleWhen`` is an ``Annotated`` alias over the discriminated union;
    # the three concrete variants are the classes callers construct.
    assert par_rt_db.AfterMs is wire.AfterMs
    assert par_rt_db.RunAt is wire.RunAt
    assert par_rt_db.Cron is wire.Cron
    assert par_rt_db.ScheduleWhen is wire.ScheduleWhen


def test_schema_exported() -> None:
    assert par_rt_db.Schema is _Schema


def test_in_memory_client_exported() -> None:
    assert par_rt_db.InMemoryRtDbClient is _InMemoryRtDbClient


def test_all_six_names_listed_in_dunder_all() -> None:
    for name in (
        "AfterMs",
        "RunAt",
        "Cron",
        "ScheduleWhen",
        "Schema",
        "InMemoryRtDbClient",
    ):
        assert name in par_rt_db.__all__, f"{name} missing from par_rt_db.__all__"


def test_underscore_aliases_still_resolve() -> None:
    # Backwards-compat: the pre-ARC-109 underscore spellings keep working so an
    # existing ``from par_rt_db.wire import _AfterMs`` import does not break.
    assert wire._AfterMs is wire.AfterMs
    assert wire._RunAt is wire.RunAt
    assert wire._Cron is wire.Cron


def test_after_ms_variants_round_trip_on_the_wire() -> None:
    # The renamed class serializes identically to before (camelCase ``type`` tag).
    from pydantic import TypeAdapter

    adapter = TypeAdapter(par_rt_db.ScheduleWhen)
    assert adapter.core_schema  # sanity: resolves as a real union
    dumped = adapter.dump_python(par_rt_db.AfterMs(ms=5000))
    assert dumped == {"type": "afterMs", "ms": 5000}
