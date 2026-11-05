"""Workflow (FM-29) and scheduled-job admin request builders, split from
``admin.py`` (QA-003b pure move)."""

from __future__ import annotations

from typing import Any

from ._admin_request import _AdminRequest, _parse_model, _parse_model_list, _parse_ok_bool
from .mutation import Transaction
from .wire import (
    ScheduleInfo,
    ScheduleWhen,
    WorkflowInfo,
    WorkflowInfoFull,
    WorkflowSpec,
    WorkflowStatus,
)

# --- workflow surface (FM-29) ---


def _op_admin_list_workflows(
    db: str,
    *,
    status: WorkflowStatus | None = None,
    limit: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {}
    if status is not None:
        params["status"] = status
    if limit is not None:
        params["limit"] = limit
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/workflows",
        {"params": params},
        _parse_model_list(WorkflowInfo, "workflows"),
    )


def _op_admin_start_workflow(db: str, spec: WorkflowSpec) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/workflows",
        {"json": spec.model_dump(by_alias=True, mode="json")},
        lambda resp: str(resp.json()["id"]),
    )


def _op_admin_get_workflow(db: str, id: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", f"/admin/db/{db}/workflows/{id}", {}, _parse_model(WorkflowInfoFull)
    )


def _op_admin_cancel_workflow(db: str, id: str) -> _AdminRequest:
    return _AdminRequest(
        "POST", f"/admin/db/{db}/workflows/{id}/cancel", {"json": {}}, _parse_ok_bool
    )


def _op_admin_signal_workflow(
    db: str, id: str, name: str, payload: Any | None = None
) -> _AdminRequest:
    body: dict[str, Any] = {"name": name}
    if payload is not None:
        body["payload"] = payload
    return _AdminRequest(
        "POST", f"/admin/db/{db}/workflows/{id}/signal", {"json": body}, _parse_ok_bool
    )


def _op_admin_delete_workflow(db: str, id: str) -> _AdminRequest:
    return _AdminRequest("DELETE", f"/admin/db/{db}/workflows/{id}", {}, _parse_ok_bool)


# --- admin schedule management ---
#
# GET|POST /admin/db/{db}/schedules + POST .../{id}/cancel|pause|resume.
# Mirrors the ts/rust admin clients one-to-one — paths, bodies, and response
# shapes identical; reuses the wire ``ScheduleInfo``/``ScheduleWhen`` and the
# DSL ``Transaction`` types the client already carries. The manage ops take the
# id + op from the path and no body; ``ok:false`` means an unknown or terminal
# id (a no-op, not an error), so they parse through ``_parse_ok_bool``.


def _op_admin_list_schedules(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", f"/admin/db/{db}/schedules", {}, _parse_model_list(ScheduleInfo, "schedules")
    )


def _op_admin_create_schedule(db: str, when: ScheduleWhen, txn: Transaction) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/schedules",
        {
            "json": {
                "when": when.model_dump(by_alias=True, mode="json"),
                "txn": txn.model_dump(by_alias=True, mode="json"),
            }
        },
        lambda resp: str(resp.json()["id"]),
    )


def _op_admin_manage_schedule(db: str, id: str, op: str) -> _AdminRequest:
    """Bodyless POST for cancel/pause/resume — ``op`` is only ever one of those
    three literals, each a path segment the server routes on."""
    return _AdminRequest("POST", f"/admin/db/{db}/schedules/{id}/{op}", {}, _parse_ok_bool)
