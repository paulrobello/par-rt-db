"""Managed-backup admin request builders, split from ``admin.py`` (QA-003b pure move)."""

from ._admin_request import (
    _AdminRequest,
    _parse_bytes,
    _parse_expect_ok,
    _parse_json_object,
    _parse_none,
)


def _op_backup_now() -> _AdminRequest:
    return _AdminRequest("POST", "/admin/backup", {"json": {}}, _parse_expect_ok)


def _op_list_backups() -> _AdminRequest:
    return _AdminRequest("GET", "/admin/backups", {}, _parse_json_object)


def _op_download_backup(name: str) -> _AdminRequest:
    return _AdminRequest("GET", f"/admin/backups/{name}", {}, _parse_bytes, raw_body=True)


def _op_delete_backup(name: str) -> _AdminRequest:
    return _AdminRequest("DELETE", f"/admin/backups/{name}", {}, _parse_none)


def _op_restore_backup(name: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/restore",
        {"json": {"name": name, "confirm": name}},
        _parse_json_object,
    )
