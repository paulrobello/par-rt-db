"""Introspection admin request builders — stats / metrics / presence /
ops-recent / config live in ``_admin_ops_db``; this module carries the audit
log (ENH-004), the live subscription inspector (ENH-010), and query
introspection (ENH-019). Split from ``admin.py`` (QA-003b pure move)."""

from __future__ import annotations

from typing import Any

from ._admin_request import _AdminRequest, _parse_model, _parse_model_list
from .admin_models import (
    AuditEntry,
    ExplainResult,
    SlowQueriesResponse,
    SubscriptionsResponse,
)
from .query import Query, TableQuery


def _op_get_audit(
    db: str,
    *,
    table: str | None = None,
    op: str | None = None,
    principal: str | None = None,
    source: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {"db": db}
    if table is not None:
        params["table"] = table
    if op is not None:
        params["op"] = op
    if principal is not None:
        params["principal"] = principal
    if source is not None:
        params["source"] = source
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    return _AdminRequest(
        "GET", "/admin/audit", {"params": params}, _parse_model_list(AuditEntry, "entries")
    )


def _op_get_subscriptions(db: str | None = None) -> _AdminRequest:
    params: dict[str, Any] = {}
    if db is not None:
        params["db"] = db
    return _AdminRequest(
        "GET",
        "/admin/subscriptions",
        {"params": params},
        _parse_model(SubscriptionsResponse),
    )


def _op_explain(db: str, query: Query | TableQuery) -> _AdminRequest:
    built = query.build() if isinstance(query, TableQuery) else query
    body = {"query": built.model_dump(by_alias=True, mode="json")}
    return _AdminRequest(
        "POST", f"/admin/db/{db}/explain", {"json": body}, _parse_model(ExplainResult)
    )


def _op_slow_queries(*, db: str | None = None, limit: int | None = None) -> _AdminRequest:
    params: dict[str, Any] = {}
    if db is not None:
        params["db"] = db
    if limit is not None:
        params["limit"] = limit
    return _AdminRequest(
        "GET", "/admin/slow-queries", {"params": params}, _parse_model(SlowQueriesResponse)
    )
