"""HTTP-shape drift verification (python-client view) over
``wire-corpus/http-shapes.json`` (repo root — the single source of truth; the
server-side parity test ``protocol::tests::http_shapes_fixture_parity``
constructs the real wire structs against it, and every client mirrors it).

Decodes the fixture's ``ClaimedSchedule`` and ``ScheduleInfo`` instances
through the pydantic wire models and requires exact camelCase field parity
with the fixture. The fixture exists because the ``ClaimedSchedule.tz`` drift
shipped undetected (fixed in 8a55d03); the WS corpus does not cover HTTP-only
shapes.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from par_rt_db.wire import ClaimedSchedule, ScheduleInfo

_FIXTURE = Path(__file__).resolve().parents[2] / "wire-corpus" / "http-shapes.json"


def _shapes() -> dict[str, Any]:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def test_claimed_schedule_cron_roundtrip() -> None:
    shapes = _shapes()
    model = ClaimedSchedule.model_validate(shapes["claimedScheduleCron"])
    # The model_serializer drops None optionals, so the dump is the exact
    # camelCase wire shape.
    assert model.model_dump(by_alias=True, mode="json") == shapes["claimedScheduleCron"]


def test_claimed_schedule_oneshot_roundtrip() -> None:
    shapes = _shapes()
    model = ClaimedSchedule.model_validate(shapes["claimedScheduleOneshot"])
    dumped = model.model_dump(by_alias=True, mode="json")
    assert dumped == shapes["claimedScheduleOneshot"]
    # Strict decoders must tolerate the absent cron/tz/everyMs keys.
    assert "cron" not in dumped
    assert "tz" not in dumped
    assert "everyMs" not in dumped
    assert model.cron is None and model.tz is None and model.every_ms is None


def test_claimed_schedule_rejects_unknown_field() -> None:
    shapes = _shapes()
    bogus = dict(shapes["claimedScheduleCron"])
    bogus["bogusKey"] = 1
    with pytest.raises(ValidationError):
        ClaimedSchedule.model_validate(bogus)


def test_schedule_info_cron_roundtrip() -> None:
    shapes = _shapes()
    model = ScheduleInfo.model_validate(shapes["scheduleInfoCron"])
    assert model.model_dump(by_alias=True, mode="json") == shapes["scheduleInfoCron"]
    assert model.external is True
    assert model.missed_count == 2
    assert model.last_missed_at == 1794470300000


def test_schedule_info_minimal_roundtrip() -> None:
    fixture = _shapes()["scheduleInfoMinimal"]
    model = ScheduleInfo.model_validate(fixture)
    assert model.model_dump(by_alias=True, mode="json") == fixture
    # Absent keys decode to their defaults and stay absent on the wire.
    assert model.external is False
    assert model.missed_count == 0
    assert model.last_missed_at is None
    assert "external" not in model.model_dump(by_alias=True, mode="json")
    assert "missedCount" not in model.model_dump(by_alias=True, mode="json")
    assert "lastMissedAt" not in model.model_dump(by_alias=True, mode="json")


def test_claim_schedules_response_shape() -> None:
    shapes = _shapes()
    model = ClaimedSchedule.model_validate(shapes["claimSchedulesResponse"]["jobs"][0])
    assert model.model_dump(by_alias=True, mode="json") == shapes["claimedScheduleCron"]
