"""Webhook management (ENH-003) request builders, including the delivery
outbox and redelivery ops. Split from ``admin.py`` (QA-003b pure move)."""

from __future__ import annotations

from typing import Any

from ._admin_request import (
    _AdminRequest,
    _parse_expect_ok,
    _parse_model,
    _parse_model_list,
    _parse_webhook_id,
)
from .admin_models import _UNSET, Webhook, WebhookDelivery


def _op_list_webhooks(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/webhooks",
        {},
        _parse_model_list(Webhook, "webhooks"),
    )


def _op_create_webhook(
    db: str,
    *,
    url: str,
    table: str | None = None,
    events: list[str] | None = None,
    enabled: bool | None = None,
) -> _AdminRequest:
    body: dict[str, Any] = {"url": url}
    if table is not None:
        body["table"] = table
    if events is not None:
        body["events"] = list(events)
    if enabled is not None:
        body["enabled"] = enabled
    return _AdminRequest("POST", f"/admin/db/{db}/webhooks", {"json": body}, _parse_webhook_id)


def _op_edit_webhook(
    db: str,
    id: int,
    *,
    url: str | None = None,
    table: str | None | object = _UNSET,
    events: list[str] | None = None,
    enabled: bool | None = None,
    rotate_secret: bool | None = None,
) -> _AdminRequest:
    body: dict[str, Any] = {}
    if url is not None:
        body["url"] = url
    if table is not _UNSET:
        # ``table`` may be ``None`` (clear to all-tables) or a string (set);
        # both are valid body values, so assign verbatim.
        body["table"] = table
    if events is not None:
        body["events"] = list(events)
    if enabled is not None:
        body["enabled"] = enabled
    if rotate_secret is not None:
        body["rotateSecret"] = rotate_secret
    return _AdminRequest(
        "PUT", f"/admin/db/{db}/webhooks/{id}", {"json": body}, _parse_model(Webhook)
    )


def _op_delete_webhook(db: str, id: int) -> _AdminRequest:
    return _AdminRequest(
        "DELETE",
        f"/admin/db/{db}/webhooks/{id}",
        {},
        _parse_expect_ok,
    )


def _op_redeliver_delivery(db: str, id: int, delivery_id: int) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/webhooks/{id}/deliveries/{delivery_id}/redeliver",
        {"json": {}},
        _parse_model(WebhookDelivery),
    )


def _op_redeliver_failed(db: str, id: int) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/webhooks/{id}/deliveries/redeliver-failed",
        {"json": {}},
        _parse_model_list(WebhookDelivery, "deliveries"),
    )


def _op_list_deliveries(
    db: str,
    id: int,
    *,
    status: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {}
    if status is not None:
        params["status"] = status
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/webhooks/{id}/deliveries",
        {"params": params},
        _parse_model_list(WebhookDelivery, "deliveries"),
    )
