"""Bulk-op HTTP-shape drift verification (python-client view) over
``wire-corpus/http-shapes.json`` (repo root — the single source of truth;
see ``tests/test_http_shapes.py`` for the fixture's provenance).

Decodes the fixture's ``bulkDeleteAccepted``/``bulkOpInfoRunning``/
``bulkOpInfoFailed`` instances through the ``BulkOpAccepted``/``BulkOpInfo``
pydantic wire models and requires exact camelCase field parity with the
fixture (ARC-005 wave 1).
"""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from par_rt_db.wire import BulkOpAccepted, BulkOpInfo

_FIXTURE = Path(__file__).resolve().parents[2] / "wire-corpus" / "http-shapes.json"


def _shapes() -> dict[str, Any]:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def test_bulk_delete_accepted_roundtrip() -> None:
    shapes = _shapes()
    model = BulkOpAccepted.model_validate(shapes["bulkDeleteAccepted"])
    assert model.model_dump(by_alias=True, mode="json") == shapes["bulkDeleteAccepted"]
    assert model.op_id == "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A"
    assert model.matched == 1500
    assert model.state == "running"


def test_bulk_op_info_running_roundtrip() -> None:
    shapes = _shapes()
    fixture = shapes["bulkOpInfoRunning"]
    model = BulkOpInfo.model_validate(fixture)
    assert model.model_dump(by_alias=True, mode="json") == fixture
    assert model.id == "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A"
    assert model.state == "running"
    assert model.matched == 1500
    assert model.deleted == 500
    # ``error`` is present ONLY on failed — the running fixture carries no
    # such key, so the model must never default it onto the wire.
    assert "error" not in model.model_fields_set
    assert "error" not in model.model_dump(by_alias=True, mode="json")
    assert model.error is None


def test_bulk_op_info_failed_roundtrip() -> None:
    shapes = _shapes()
    model = BulkOpInfo.model_validate(shapes["bulkOpInfoFailed"])
    assert model.model_dump(by_alias=True, mode="json") == shapes["bulkOpInfoFailed"]
    assert model.state == "failed"
    assert model.error == "chunk turn failed: schema changed mid-op"
    assert "error" in model.model_fields_set


def test_bulk_op_info_rejects_unknown_field() -> None:
    shapes = _shapes()
    bogus = dict(shapes["bulkOpInfoRunning"])
    bogus["bogusKey"] = 1
    with pytest.raises(ValidationError):
        BulkOpInfo.model_validate(bogus)


def test_bulk_op_accepted_rejects_unknown_field() -> None:
    shapes = _shapes()
    bogus = dict(shapes["bulkDeleteAccepted"])
    bogus["bogusKey"] = 1
    with pytest.raises(ValidationError):
        BulkOpAccepted.model_validate(bogus)
