"""Schema admin request builders (push / preview / get / JSON-Schema / migrate /
history / restore), split from ``admin.py`` (QA-003b pure move)."""

from __future__ import annotations

from ._admin_request import (
    _AdminRequest,
    _parse_expect_ok,
    _parse_json_object,
    _parse_model,
    _parse_model_list,
    _parse_restored_version,
)
from .admin_models import (
    MigrateResult,
    SchemaHistoryEntry,
    SchemaHistorySummary,
    SchemaPreviewDiff,
)
from .migration import Directive, MigrateRequest
from .schema import SchemaDef


def _op_push_schema(db: str, schema: SchemaDef) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/push-schema",
        {"json": {"db": db, "schema": schema.model_dump(by_alias=True, mode="json")}},
        _parse_expect_ok,
    )


def _op_preview_schema(db: str, schema: SchemaDef) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/schema/preview",
        {"json": {"schema": schema.model_dump(by_alias=True, mode="json")}},
        _parse_model(SchemaPreviewDiff),
    )


def _op_get_schema(db: str) -> _AdminRequest:
    return _AdminRequest("GET", f"/admin/dbs/{db}/schema", {}, _parse_model(SchemaDef))


def _op_get_json_schema(db: str) -> _AdminRequest:
    return _AdminRequest("GET", f"/admin/db/{db}/schema/json-schema", {}, _parse_json_object)


def _op_migrate_schema(
    db: str,
    directives: list[Directive] | MigrateRequest,
    *,
    dry_run: bool,
) -> _AdminRequest:
    wire_directives = (
        directives.directives if isinstance(directives, MigrateRequest) else directives
    )
    body = {
        "directives": [d.model_dump(by_alias=True, mode="json") for d in wire_directives],
        "dryRun": dry_run,
    }
    return _AdminRequest(
        "POST", f"/admin/db/{db}/migrate", {"json": body}, _parse_model(MigrateResult)
    )


def _op_list_schema_history(
    db: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> _AdminRequest:
    params: dict[str, int] = {}
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/schema/history",
        {"params": params or None},
        _parse_model_list(SchemaHistorySummary, "entries"),
    )


def _op_get_schema_version(db: str, version: int) -> _AdminRequest:
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/schema/history/{version}",
        {},
        _parse_model(SchemaHistoryEntry),
    )


def _op_restore_schema(db: str, version: int, *, confirm: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/schema/restore",
        {"json": {"version": version, "confirm": confirm}},
        _parse_restored_version,
    )
